FROM python:3.11-slim

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir .

CMD ["python", "-m", "fixed_time.cli", "live-run", "--root", "/app"]
