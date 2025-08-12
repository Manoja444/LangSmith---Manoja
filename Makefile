.PHONY: db-up db-down db-migrate db-downgrade db-migrate-test server format lint lint-fix test test-setup benchmark benchmark-compare benchmark-compare-save memprofile

# Start all database services defined in docker-compose-db.yaml
db-up:
	docker-compose -f docker-compose-db.yaml --profile postgres-15 up -d

# Stop all database services defined in docker-compose-db.yaml
db-down:
	docker-compose -f docker-compose-db.yaml down

# Run database migrations
db-migrate:
	poetry run alembic upgrade head

# Run migrations with test environment
db-migrate-test:
	RUN_HANDLER_ENV=test poetry run alembic upgrade head

# Downgrade database to previous migration
db-downgrade:
	poetry run alembic downgrade -1

# Run migrations and start the server
server: db-migrate
	poetry run uvicorn ls_py_handler.main:app --reload

# Format code using Ruff
format:
	poetry run ruff format ls_py_handler tests

# Lint code using Ruff
lint:
	poetry run ruff check ls_py_handler tests

# Automatically fix linting issues using Ruff
lint-fix:
	poetry run ruff check --fix ls_py_handler tests

# Set up test environment (reset database and S3 bucket)
test-setup:
	@echo "Setting up test environment..."
	@echo "1. Dropping postgres_test database if it exists..."
	-docker exec -it ls-py-run-handler-db-postgres-15-1 psql -U postgres -c "DROP DATABASE IF EXISTS postgres_test;"
	@echo "2. Creating postgres_test database..."
	docker exec -it ls-py-run-handler-db-postgres-15-1 psql -U postgres -c "CREATE DATABASE postgres_test;"
	@echo "3. Configuring MinIO client..."
	docker exec -it ls-py-run-handler-minio-1 mc alias set local http://localhost:9000 minioadmin1 minioadmin1
	@echo "4. Clearing and recreating runs-test bucket..."
	-docker exec -it ls-py-run-handler-minio-1 mc rb --force local/runs-test
	docker exec -it ls-py-run-handler-minio-1 mc mb local/runs-test
	@echo "5. Running migrations on test database..."
	make db-migrate-test
	@echo "Test environment setup complete!"

# Run tests with test environment
test: test-setup
	RUN_HANDLER_ENV=test poetry run pytest -s

# Run performance benchmarks
benchmark: test-setup
	@echo "Running performance benchmarks..."
	RUN_HANDLER_ENV=test poetry run pytest tests/benchmarks/test_run_performance.py -v --benchmark-save=baseline --benchmark-compare

# Run memory profiling
memprofile: test-setup
	@echo "Running memory profiling..."
	RUN_HANDLER_ENV=test poetry run pytest tests/benchmarks/test_run_performance.py -v --memray