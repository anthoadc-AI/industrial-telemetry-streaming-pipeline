# Diagnostic image (Task 3.3 STEP 0, disposable): the probes inside the
# Docker Desktop VM, on the compose network. Same Python and the same
# confluent-kafka (bundled librdkafka 2.15.1) as the host's .venv.
#   docker build -t bench-diag:local scripts/bench/diagnostics
FROM python:3.13.4-slim-bookworm
RUN pip install --no-cache-dir confluent-kafka==2.15.1
WORKDIR /diag
COPY produce_ack_probe.py clock_offset.py ./
