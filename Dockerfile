FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      gdal-bin libgdal-dev \
      libproj-dev proj-bin proj-data \
      libgeos-dev \
      libspatialindex-dev \
      curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Poetry + deps
COPY pyproject.toml poetry.lock* README.md ./
# Copia el paquete para que Poetry pueda instalar el proyecto raíz
COPY jat_slides ./jat_slides

RUN pip install --no-cache-dir "poetry==2.1.3" \
 && poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-ansi --only main

# Project code
COPY . .

# Expose the gRPC port used by the code location (compose maps it)
EXPOSE 4001

# NOTE: Do NOT set DAGSTER_HOME here; the instance (webserver/daemon) owns it.
# The container is started by docker-compose with:
#   dagster api grpc -m jat_slides.definitions --host 0.0.0.0 --port 4001
# so we keep CMD empty and let compose provide the command.