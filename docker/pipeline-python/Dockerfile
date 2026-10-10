# The Python components of the pipeline in one image: the MQTT-to-Kafka
# bridge, the ThingsBoard sink, the edge simulator and the Kafka topic
# provisioning. Each compose service picks its module with `command`.
#
# Build from the repository root (docker-compose.yml does):
#   docker compose --profile pipeline build
#
# Python 3.13.4, as .python-version; the base is pinned by digest, so a
# rebuild uses the same OS layer. Dependencies are requirements.txt, the same
# exact pins as the host .venv. The image holds no credential: .dockerignore
# is an allowlist, and settings arrive as environment variables.
FROM python:3.13.4-slim-bookworm@sha256:9ed09f78253eb4f029f3d99e07c064f138a6f1394932c3807b3d0738a674d33b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app

COPY common/ common/
COPY config/ config/
COPY schemas/ schemas/
COPY bridge/ bridge/
COPY edge/ edge/
COPY broker/ broker/
COPY processing/thingsboard_sink/ processing/thingsboard_sink/

USER app
