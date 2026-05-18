# ============================================================================
# Base stage
# ============================================================================
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.2 AS base

WORKDIR /opt

ARG POETRY_VERSION="1.8.3"

ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_HOME=/opt/poetry \
    VIRTUAL_ENV=/opt/openeo_argoworkflows_executor/.venv \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/opt/openeo_argoworkflows_executor/.venv/bin:$PATH

# Build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    build-essential \
    python3-dev \
    python3-venv \
    libexpat1 \
    libexpat1-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for Docker layer caching
COPY README.md pyproject.toml poetry.lock /opt/

# Create virtual environment and install Poetry
RUN python3 -m venv $VIRTUAL_ENV && \
    $VIRTUAL_ENV/bin/pip install --upgrade pip setuptools wheel && \
    $VIRTUAL_ENV/bin/pip install poetry==${POETRY_VERSION}

# Install Python dependencies
RUN poetry install --only main --no-interaction --no-ansi

# ============================================================================
# Production stage
# ============================================================================
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.2 AS prod

ARG USER_ID=1000
ARG GROUP_ID=1000

WORKDIR /opt/openeo_argoworkflows_executor

ENV VIRTUAL_ENV=/opt/openeo_argoworkflows_executor/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Minimal runtime packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    libexpat1 \
    libexpat1-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=base \
    /opt/openeo_argoworkflows_executor/.venv \
    /opt/openeo_argoworkflows_executor/.venv

# Copy application source code
COPY ./openeo_argoworkflows_executor \
    /opt/openeo_argoworkflows_executor

# Create executor user
RUN groupadd -g ${GROUP_ID} -o executor && \
    useradd -m -u ${USER_ID} -g ${GROUP_ID} -o -s /bin/bash executor && \
    chown -R ${USER_ID}:${GROUP_ID} /opt/openeo_argoworkflows_executor

USER ${USER_ID}

CMD ["openeo_executor"]
