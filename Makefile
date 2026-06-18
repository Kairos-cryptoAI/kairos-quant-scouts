.PHONY: install lint test format run
install:
	pip install -e ".[dev]"
format:
	ruff format kairos_quant tests
lint:
	ruff check kairos_quant tests
test:
	pytest -q
run:
	python -m kairos_quant
