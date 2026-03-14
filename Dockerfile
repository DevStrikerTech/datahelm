FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip \
    && pip install -e .

ENV DAGSTER_HOME=/app/.dagster_home
RUN mkdir -p "${DAGSTER_HOME}"

EXPOSE 3000

CMD ["python", "-m", "dagster", "api", "grpc", "-m", "dagster_op.repository"]
