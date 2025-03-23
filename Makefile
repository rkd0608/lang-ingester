.PHONY: db-up db-down db-migrate db-downgrade server format lint lint-fix

# Start all database services defined in docker-compose-db.yaml
db-up:
	docker-compose -f docker-compose-db.yaml --profile postgres-15 up -d

# Stop all database services defined in docker-compose-db.yaml
db-down:
	docker-compose -f docker-compose-db.yaml down

# Run database migrations
db-migrate:
	poetry run alembic upgrade head

# Downgrade database to previous migration
db-downgrade:
	poetry run alembic downgrade -1

# Format code using Ruff
format:
	poetry run ruff format ls_py_handler tests

# Lint code using Ruff
lint:
	poetry run ruff check ls_py_handler tests

# Automatically fix linting issues using Ruff
lint-fix:
	poetry run ruff check --fix ls_py_handler tests

# Run migrations and start the server
server: db-migrate
	poetry run uvicorn ls_py_handler.main:app --reload