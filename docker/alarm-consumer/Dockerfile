# The Node.js alarm consumer.
#
# Build from the repository root (docker-compose.yml does):
#   docker compose --profile pipeline build
#
# Node.js 24.16.0, as .nvmrc; the base is pinned by digest. Production
# dependencies only, installed from package-lock.json (npm ci). Runs as the
# image's unprivileged `node` user. The image holds no credential:
# .dockerignore is an allowlist, and settings arrive as environment variables.
FROM node:24.16.0-bookworm-slim@sha256:2c87ef9bd3c6a3bd4b472b4bec2ce9d16354b0c574f736c476489d09f560a203

ENV NODE_ENV=production

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-fund --no-audit && npm cache clean --force

COPY common/jsonlog.js common/
COPY config/ config/
COPY schemas/ schemas/
COPY processing/alarm_consumer/ processing/alarm_consumer/

USER node

CMD ["node", "processing/alarm_consumer/index.js"]
