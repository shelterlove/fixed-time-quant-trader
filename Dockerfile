FROM python:3.11-slim AS base
ARG SOURCE_REVISION=unknown
ENV SOURCE_REVISION=$SOURCE_REVISION

WORKDIR /app
COPY pyproject.toml strategy.toml testnet.toml README.md /app/
COPY src /app/src
RUN pip install --no-cache-dir .

FROM base AS test
RUN pip install --no-cache-dir '.[test]'
COPY tests /app/tests
COPY deploy.sh /app/deploy.sh
CMD ["python", "-m", "pytest", "-q"]

FROM base AS production
CMD ["python", "-m", "fixed_time.cli", "live-run", "--root", "/app"]
