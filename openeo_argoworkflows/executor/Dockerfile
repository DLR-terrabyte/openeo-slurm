FROM python:3.11-slim-bookworm as base 
# adapted to slim-bookworm to match prod

WORKDIR /opt

### add gdal for ML processes
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    build-essential \
&& rm -rf /var/lib/apt/lists/*
###

ARG POETRY_VERSION="1.8.3"

ENV POETRY_VIRTUALENVS_IN_PROJECT 1
ENV POETRY_HOME=/opt/poetry
ENV VIRTUAL_ENV=/opt/openeo_argoworkflows_executor/.venv

COPY ./README.md ./pyproject.toml ./poetry.lock /opt/

RUN python3 -m venv $VIRTUAL_ENV && \
    $VIRTUAL_ENV/bin/pip install -U pip setuptools && \
    $VIRTUAL_ENV/bin/pip install poetry==${POETRY_VERSION}

ENV PATH="${POETRY_HOME}/bin:${VIRTUAL_ENV}/bin:${PATH}"

RUN poetry install --only main
########################################################################################################################
FROM python:3.11-slim-bookworm AS prod

ARG USER_ID=1000
ARG GROUP_ID=1000

WORKDIR /opt/openeo_argoworkflows_executor

### add gdal for ML processes
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libexpat1 \
    libexpat1-dev \
 && rm -rf /var/lib/apt/lists/*
###

COPY --from=base --chown=${USER_ID}:${GROUP_ID} /opt/openeo_argoworkflows_executor/.venv /opt/openeo_argoworkflows_executor/.venv
COPY --chown=${USER_ID}:${GROUP_ID} ./openeo_argoworkflows_executor /opt/openeo_argoworkflows_executor

# Need to update the symlinks to the python interpreter in the venv to the new location.
RUN ln -sf /usr/local/bin/python /opt/openeo_argoworkflows_executor/.venv/bin/python \
    && ln -sf /usr/local/bin/python3 /opt/openeo_argoworkflows_executor/.venv/bin/python3 \
    && ln -sf /usr/local/bin/python3.9 /opt/openeo_argoworkflows_executor/.venv/bin/python3.9 \
    && groupadd -g ${GROUP_ID} -o executor \
    && useradd -m -u ${USER_ID} -g ${GROUP_ID} -o -s /bin/bash executor

ENV VIRTUAL_ENV=/opt/openeo_argoworkflows_executor/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN apt-get update && apt-get install -y libexpat1 libexpat1-dev

USER ${USER_ID}

CMD ["openeo_executor"]
