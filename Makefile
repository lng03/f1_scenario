.PHONY:  build test run

build:
	@docker build -t f1_pipeline:latest .

test:
	@docker run --entrypoint "python" f1_pipeline:latest -m unittest

run:
	@docker run \
		-v $(shell pwd)/output/:/pipeline/output \
		--entrypoint "python" \
		-it f1_pipeline:latest process.py

interactive:
	@docker run \
	-v $(shell pwd)/:/pipeline/ \
	--entrypoint "/bin/bash" \
	-it f1_pipeline:latest