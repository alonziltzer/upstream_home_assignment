SHELL := /bin/bash

DATA_LAKE := data_lake

.PHONY: docker-build docker-run docker-test clean help

help:
	@echo "Usage: make <target>"
	@echo
	@echo "Available targets:"
	@echo "  docker-build   build Docker image"
	@echo "  docker-run     run Docker container and execute pipeline"
	@echo "  docker-test    run tests inside Docker container"
	@echo "  clean          remove data_lake"

docker-build:
	@echo "🛠️ Building Docker image..."
	docker build -t upstream-task .

docker-run:
	@echo "🚀 Running Docker container..."
	docker run --rm -it -v $(PWD):/app upstream-task


docker-stage:
	docker run --rm -it -v $(PWD):/app upstream-task dagster job execute -f pipeline/pipeline_dagster.py -j data_pipeline --op $(stage)

docker-test:
	@echo "🧪 Running tests inside Docker container..."
	docker run --rm -it -v $(PWD):/app upstream-task pytest tests

clean:
	@echo "🧽 Cleaning data lake..."
	@rm -rf $(DATA_LAKE)
	@echo "✅ Cleaned."
