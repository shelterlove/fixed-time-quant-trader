FROM python:3.11-slim
ARG SOURCE_REVISION=unknown
ENV SOURCE_REVISION=$SOURCE_REVISION

WORKDIR /app
COPY pyproject.toml strategy.toml testnet.toml README.md /app/
COPY src /app/src
COPY seed /app/seed
RUN pip install --no-cache-dir .

CMD ["python", "-m", "fixed_time.cli", "live-run", "--root", "/app"]
