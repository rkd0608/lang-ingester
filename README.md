# ls-py-handler

A simple FastAPI server with run endpoints for handling asynchronous operations.

## Features

- `POST /runs` endpoint to create new runs
- `GET /runs/{id}` endpoint to retrieve run information by UUID

## Setup

This project uses Poetry for dependency management.

```bash
# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

## Running the Server

```bash
# Start the server
poetry run uvicorn ls_py_handler.main:app --reload
```

The API will be available at http://localhost:8000

## API Documentation

Once the server is running, you can access the auto-generated API documentation at:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Testing

```bash
# Run tests
poetry run pytest