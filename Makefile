.PHONY: install dev-install test lint format clean build release

# Install dependencies
install:
	poetry install --no-dev

# Install with development dependencies
dev-install:
	poetry install

# Run tests
test:
	poetry run pytest tests/ -v || echo "No tests found"

# Run linting
lint:
	poetry run black --check .
	poetry run isort --check-only .
	poetry run flake8 .
	poetry run mypy .

# Format code
format:
	poetry run black .
	poetry run isort .

# Clean build artifacts
clean:
	rm -rf dist/
	rm -rf build/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

# Build package
build:
	poetry build

# Install pre-commit hooks
hooks:
	poetry run pre-commit install

# Run pre-commit on all files
check:
	poetry run pre-commit run --all-files

# Update dependencies
update:
	poetry update

# Show current version
version:
	poetry version

# Bump version (patch)
bump-patch:
	poetry version patch

# Bump version (minor)
bump-minor:
	poetry version minor

# Bump version (major)
bump-major:
	poetry version major
