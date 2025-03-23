# ls-py-handler

A simple FastAPI server with run endpoints for handling asynchronous operations.

## Features

- `POST /runs` endpoint to create new runs
- `GET /runs/{id}` endpoint to retrieve run information by UUID

## Quick Start

```bash
# 1. Install dependencies
poetry install

# 2. Start database services (required before running the server)
make db-up

# 3. Run migrations and start the server
make server
```

The API will be available at http://localhost:8000

## Setup Details

This project uses Poetry for dependency management.

```bash
# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

## Database Setup

This project uses PostgreSQL for data storage and MinIO for object storage. Docker Compose is used to manage these services.

```bash
# Start database services (PostgreSQL and MinIO)
make db-up

# Stop database services
make db-down

# Run database migrations
make db-migrate

# Revert the most recent migration
make db-downgrade
```

## Running the Server

```bash
# Start the server with migrations applied
make server

# Or manually start the server
poetry run uvicorn ls_py_handler.main:app --reload
```

## API Documentation

Once the server is running, you can access the auto-generated API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Testing

```bash
# Run tests
poetry run pytest