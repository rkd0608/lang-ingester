# Submission Notes

## Overview

Project name:
`lang-ingester`

Goal of this optimization pass:
`Optimize performance wrt memory usage and speed.`

Short summary of the outcome:
`Memory usage and speed performance improved overall.`

## Changes Made

### 1. High-Level Changes

1. I eliminated per request connection creation to s3 and pg which debloated the memory usage for each client instance and also the latency for each round trip for creating the connection with the db.

2. I replaced N sequential INSERT calls per run with a single executemany inside one transaction. This reduced N Postgres round-trips to 1 regardless of batch size and made partial batch commits on failure impossible. So all runs in a batch now commit atomically. 

3. I identified and fixed a silent correctness bug in the offset computation. The original bytes.find() approach returns the first occurrence of a byte sequence in the batch blob. If two runs share identical field data (e.g. both have {"model": "gpt-4"} as metadata), all subsequent runs store the wrong offset
and silently return another run's data on GET. I replaced this with deterministic position tracking where I am recording field_start = len(buffer) before writing each field and field_end = len(buffer) after, making collisions structurally impossible.

4. I switched the S3 batch format from a JSON array ([{...},{...}]) to NDJSON ({...}\n{...}\n) and stored each run's byte range (object_start, object_end) in Postgres. This reduced GET from 3 parallel S3 reads (one per field) down to 1 single byte-range read covering the full run.

5. I eliminated the 32MB full-batch re-serialization blob. The original code called orjson.dumps(run_dicts) to serialize the entire batch into one bytes object, then searched it with bytes.find(). I replaced this with an incremental bytearray build. Each run is serialized once and appended directly, tracking
positions as it goes. This reduced peak write memory from 191MB to 157MB.

6. I added streaming JSON parsing via ijson to replace FastAPI's full request body buffering. Instead of Starlette buffering the entire request body before the handler runs, the request is now parsed incrementally as it streams in using AsyncRequestStreamReader + ijson.items_async. Each run is validated with
Pydantic as it comes off the wire. This eliminated the 16.5MB Starlette body buffer allocation and brought peak write memory down from 157MB to 107MB. The tradeoff is slightly higher write latency due to ijson's per-event parsing overhead compared to orjson's bulk deserialization.

7. I added a Redis cache layer on the read path. On GET, Redis is checked first. A cache hit returns immediately without touching Postgres or S3. On a miss, the run is fetched from DB and S3 and then stored in Redis for future requests. A payload size gate (should_cache_payload) prevents large runs from
bloating the cache. All Redis calls are wrapped in try/except so a Redis outage degrades gracefully to the DB + S3 path without impacting availability.

### 2. API / Request Path Changes

- `POST /runs` and `GET /runs/{id}` now acquire a DB connection from a shared `asyncpg` pool and use a shared persistent S3 client injected via `app.state` — no per-request connection setup.
- `POST /runs` no longer accepts `List[Run]` via FastAPI's body parsing. It now takes `Request` directly and parses the JSON array incrementally using `ijson` streaming — runs are validated with Pydantic one at a time as they come off the wire, without buffering the full request body into memory first.
- `POST /runs` serializes each run once and appends it directly to an incremental `bytearray` batch buffer — no full-batch re-serialization pass.
- `GET /runs/{id}` now checks Redis first. On a cache hit it returns immediately without touching Postgres or S3. On a miss it performs one DB lookup followed by one S3 byte-range read covering the full run, then populates the cache for future requests.
- The response for `GET /runs/{id}` is the full run JSON object exactly as stored in the NDJSON batch file — the shape is identical to the original response.
- The deprecated `@app.on_event("startup")` handler was replaced with a proper `@asynccontextmanager lifespan` that initialises and tears down the pool, S3 client, and Redis client cleanly on startup and shutdown.

### 3. Database Changes

- Added a new Alembic migration (`5ce3f7f7ff52_add_object_range_columns`) that adds three columns to the `runs` table:
  - `object_key TEXT` — the S3 key of the NDJSON batch file containing this run
  - `object_start BIGINT` — byte offset where this run's JSON object begins in that file
  - `object_end BIGINT` — byte offset where this run's JSON object ends
- The original `inputs`, `outputs`, and `metadata` TEXT columns (which stored S3 reference strings) are no longer populated by new writes. The three new columns replace them as the retrieval index.
- The INSERT statement was changed from N individual `fetchval(INSERT ... RETURNING id)` calls in a loop to a single `executemany(INSERT ...)` call inside one `async with conn.transaction()` block — all runs in a batch commit atomically.

### 4. Object Storage Changes

- The batch file format changed from a JSON array (`[{run1},{run2}]`) to NDJSON (`{run1}\n{run2}\n`) — one complete JSON object per line with no wrapping array structure.
- The S3 object key extension changed from `.json` to `.ndjson` and the `ContentType` from `application/json` to `application/x-ndjson`.
- The write path no longer uses `orjson.dumps(run_dicts)` to produce a single large batch blob. Instead each run is serialized individually and appended to an incremental `bytearray` — this eliminated the 32MB peak allocation that previously existed for a 500-run batch.
- On the read path, `GET /runs/{id}` now issues a single byte-range request (`Range: bytes={object_start}-{object_end-1}`) to fetch the full run JSON in one round-trip, down from three parallel byte-range requests (one per tracked field) in the original implementation.

### 5. Benchmark / Test Changes

`These are provided in the PR with a writeup.`

## Thought Process And Approach

### 1. Initial Problems Identified

`I did an initial run before making any changes and recorded both memprofile and performance benchmarks. 
Based on benchmarks reports, I went through the codebase to understand and identify these bottlenecks. 
Here are the initial notes I gathered down below: `

### Inefficiencies: 

1. Serialization on every run in a batch. 
2. 1 write per run on postgres and 1 write in s3 for the whole file.  1 Write per run -> O(n) for pg + O(1) writes per batch for s3
3. Deserialization on every run in a batch. 
4. 1 read from postgres and 3 read from s3 -> O(1) + O(3) reads per batch
5. .find() might have collisions if multiple run data contain the same information. for example gpt-4/gpt-3 might have same start and end position and the data returned will be wrong. 
6. Creating a connection with postgres and s3 in every request. This is an extra overhead which could be minimized by just creating a connection once when the app is started or deployed and then utilize those.



### 2. Implementation Strategy  & Why I Chose This Approach

1. I planned a bottom up approach. Fix the easy things first and then move on to the more complex things.`

2. I noticed that initially the db connection with postgres and s3 was being created with every new requested, which in 
turn was increasing latency due to each time a new connection handshake was being created. Hence, to tackle that, I went ahead with the connection pool which creates the connection 
once during application starts up. It instantly dropped the READ latency from 99.9ms to 3.6ms for 10KB`.

3. Next, I had identified that for each run, we are sequentially doing a write operation. This was the second bottleneck 
that could be optimized. Instead of doing N transactions for N runs, we could do 1 transaction for each batch into pg. And S3 
was receiving 1 write operation per batch anyway so all good there.

4. Then, there was a correctness bug when calculating for offsets. Original implementation used bytes.find() to locate the field position (input, output, etc). This will fail silently for runs with same payload with wrong offset calculation and will return wrong data on GET calls.

5. The original batch format was a JSON array ([{...},{...}]). I switched to NDJSON ({...}\n{...}\n) where each run is a self-contained JSON object on its own line. This enabled storing a single byte range per run (object_start, object_end) in Postgres instead of three separate per-field references. On the read path, this reduced S3 fetches from 3 parallel reads (one per field) down to 1 read covering the full run.

6. Even after the bytearray build improvement, peak write memory was still high because Starlette was buffering the entire request body into memory before the handler ran. I replaced FastAPI's `List[Run]` body parsing with a streaming approach using `ijson` — the request body is now parsed incrementally as it streams in. Each run is validated with Pydantic as it comes off the wire. This eliminated the 16.5MB Starlette body buffer and brought peak write memory from 157MB down to 107MB. The tradeoff is slightly higher write latency since `ijson` parses event-by-event rather than bulk-deserializing with `orjson`.

7. Finally, I added a Redis cache layer on the read path. Since GET latency was already low after the connection pool and single S3 read improvements, the cache targets the hot-path use case — repeatedly fetching the same run. On GET, Redis is checked first and a cache hit returns without touching Postgres or S3 at all. A payload size gate prevents large runs from bloating the cache and all Redis calls fail gracefully so a Redis outage does not affect availability.

### 4. Tradeoffs

1. In memory cache -> speed will be slightly better but memory usage will increase. Went with redis cache to store most recently used runs with LRU eviction policy and a TTL. Speed will be better and memory usage wont spike. Further, cache can be scaled independently.
2. Huge batch size  processing in memory -> Can be very expensive if we receive a batch burst consisting of a large number of runs in it. 
Memory usage will spike dramatically and CPU consumption will spike as well. I went with incremental streaming where we receive the runs of a batch incrementally.
3. I integrated redis cache for pre-warmup of cache data on POST request and retrieval of cache data on GET request. The tradeoff is that the cache comes at a cost of writes operation for every single batch request which degraded speed performance for POST requests. (before mean: 300.3405 (120.49) after mean: 660.3151 (264.89)). It more than doubled.
4. So next I went with populate cache on read miss to avoid extra write overhead.   

## Performance Results

Baseline reference:
All comparisons are against `0001_baseline` i.e the original implementation with no changes applied.

Final benchmark summary:

```text
GET 10KB:
  - 0001_baselin mean: 99.8774 ms
  - NOW mean:          4.1979 ms
  - Speedup:           ~24× faster

GET 100KB:
  - 0001_baselin mean: 105.9713 ms
  - NOW mean:          11.7835 ms
  - Speedup:           ~9× faster

POST 50×100KB:
  - 0001_baselin mean: 537.8868 ms
  - NOW mean:          371.2245 ms
  - Speedup:           ~1.4× faster

POST 500×10KB:
  - 0001_baselin mean: 1,487.4929 ms
  - NOW mean:            381.4372 ms
  - Speedup:             ~3.9× faster
```

All timings are mean latency from `make benchmark` (`pytest-benchmark`, min 5 rounds).
The GET improvement is the most dramatic — driven by connection pooling (99ms → 4ms in one step)
and then further reduced by the NDJSON single-range read.
The POST improvement is driven primarily by replacing N sequential inserts with a single
`executemany` inside one transaction, eliminating the full-batch re-serialization pass, and
switching to streaming JSON parsing via `ijson` which trades some write latency for significantly
lower peak memory usage.

Memory profile summary (peak allocation at high watermark, from `make memprofile`):

```text
┌───────────────┬──────────┬─────────┬─────────────────────────────────────────────────────────────────────┐
│     Test      │ Original │ Now     │ Change                                                              │
├───────────────┼──────────┼─────────┼─────────────────────────────────────────────────────────────────────┤
│ POST 500×10KB │ 191.4MB  │ 107.2MB │ -44% (streaming parser eliminated full request body buffer)         │
├───────────────┼──────────┼─────────┼─────────────────────────────────────────────────────────────────────┤
│ POST 50×100KB │ 133.4MB  │ 108.1MB │ -19% (same reason — Starlette body buffer gone)                     │
├───────────────┼──────────┼─────────┼─────────────────────────────────────────────────────────────────────┤
│ GET 10KB      │ 14.6MB   │ 9.3MB   │ -36% (botocore 3.3MB init per request eliminated)                   │
├───────────────┼──────────┼─────────┼─────────────────────────────────────────────────────────────────────┤
│ GET 100KB     │ 8.1MB    │ 10.1MB  │ +25% (response render regression — orjson.loads + FastAPI serialize) │
└───────────────┴──────────┴─────────┴─────────────────────────────────────────────────────────────────────┘
```

The write-path memory improvement comes from two changes: eliminating the 32MB `orjson.dumps`
batch blob replaced by an incremental bytearray build, and switching to `ijson` streaming which
eliminated the 16.5MB Starlette request body buffer entirely.
The GET improvement comes from eliminating the 3.3MB botocore service model initialization
that was happening on every request due to per-request S3 client creation.
The GET 100KB memory regression is a known side effect of returning `orjson.loads(data)`
instead of raw bytes — FastAPI re-serializes the parsed dict for the response. Fixable by
returning `Response(content=data, media_type="application/json")` directly.

Key takeaways:
- Connection pooling was the single highest-impact change — responsible for the majority of the GET latency improvement.
- The correctness bug (`bytes.find()` collisions) was invisible in benchmarks but would corrupt data silently in production for any two runs sharing identical field payloads.
- Switching to NDJSON and storing run-level byte ranges reduced S3 reads per GET from 3 to 1, with measurable improvement on larger payloads.
- Redis caching improved GET latency for small runs on warm cache hits but added write overhead for large batches — the tradeoff is documented in the Tradeoffs section above.
- Write performance scales with total data volume rather than run count after the `executemany` change — the 500-run and 50-run batches now perform similarly since they carry the same total payload size.

## Future Improvements

### Ideas I Would Implement Next

- I would think about how this could be used in production by real users. Real users will try searching by trace id for runs how splunk does it. I would index the trace_id column. It will come with a tradeoff of a little increased write latency that I would keep in mind. 
- Integrate elastic search with reverse indexing of traces by keywords can significantly improve user experience for debugging steps by keywords. 

### Ideas Considered But Not Implemented

1. I considered between in memory cache for even faster performance, but I traded performance speed with memory usage. 
2. I considered multipart streaming of runs objects to S3 incrementally (how we iteratively stream the request) but did not implement due to time constraints. 

### Remaining Risks Or Limitations

1. The POST request doesn't validate the size of the batch. Although S3 could handle significant large batch sizes, we still should validate the size of the batch to avoid memory overflow. This isn't a pressing issue and highly unlikely to occur, its worth keeping in mind. 
2. Currently, we only query GET request by the uidpk of PG which is fine because it is indexed. In the future, if we ever want to improve the system to query by trace id, we can add indexing on trace id as well. 
3. Further, if we want to improve our search capabilities, to track down traces by keywords, I would suggest an elasticsearch integration with reverse indexing so users will be able to find out Run documents and traces by searching through keywords. 


