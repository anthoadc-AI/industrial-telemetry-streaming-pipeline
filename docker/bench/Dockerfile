# The benchmark's generator and meter (Phase 3, Task 3.3): the pipeline's Python
# image plus bench/. Benchmark-only, so the product image stays free of it.
# bench/harness.py builds it after the pipeline images:
#   docker build -t industrial-telemetry/bench:local -f docker/bench/Dockerfile .
FROM industrial-telemetry/pipeline-python:local

COPY bench/*.py bench/
