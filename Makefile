UV ?= uv

.PHONY: install lint format format-check typecheck security test build run all
install:
	$(UV) sync --locked
format:
	$(UV) run --locked ruff format kairos_quant tests
format-check:
	$(UV) run --locked ruff format --check kairos_quant tests
lint:
	$(UV) run --locked ruff check kairos_quant tests
typecheck:
	$(UV) run --locked mypy kairos_quant
security:
	$(UV) run --locked bandit -q -r kairos_quant -x tests
test:
	$(UV) run --locked pytest -q --tb=short
build:
	$(UV) build --no-sources
run:
	$(UV) run --locked python -m kairos_quant
all: lint format-check typecheck security test build
