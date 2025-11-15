# Dockerfile - API (improved)
FROM python:3.9-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential \
    gdal-bin \
    libgdal-dev \
    proj-bin \
    libproj-dev \
    libgeos-dev \
    pkg-config \
    wget \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV GDAL_LIBRARY_PATH=/usr/lib

WORKDIR /app

COPY src/api/requirements.txt ./requirements.txt

RUN pip install --upgrade pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

COPY src/api/main.py ./main.py
COPY src/common/ ./common

EXPOSE 80

ENV AWS_NO_SIGN_REQUEST=NO
# default value for local/testing — override in ECS task def or env
ENV TITILER_ENDPOINT=http://localhost:8080/viz
ENV API_BASE_URL=http://localhost:80

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s \
  CMD curl -f http://localhost:80/health || exit 1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80", "--proxy-headers"]
