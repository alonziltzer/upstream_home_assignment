SHELL := /bin/bash

CONDA_ENV := upstream_home_assignment

ENV_FILE := environment.yml
.PHONY: help create test

create:
	@echo "Creating conda environment..."
	bash -c "conda env remove -n $(CONDA_ENV) -y || true; conda env create -f $(ENV_FILE) -n $(CONDA_ENV)"

test:
	@echo "Running black..."
	conda run -n $(CONDA_ENV) python -m black --check pipeline tests
	@echo "Running flake8..."
	conda run -n $(CONDA_ENV) python -m flake8 pipeline tests
	@echo "Running pytest...    "
	conda run -n $(CONDA_ENV) python -m pytest -v tests