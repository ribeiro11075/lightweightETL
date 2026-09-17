# The `understudy` command, with every database driver.
#
#   docker build -t understudy-data .
#   docker run --rm -v "$PWD/configuration:/work/configuration" -e MASKING_KEY understudy-data run
#
# The working directory is /work; mount the configuration there, and a volume
# for run state if it should outlive the container.

FROM python:3.14-slim AS build

# psycopg2 compiles against libpq; the other drivers ship wheels.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY understudy_data ./understudy_data
RUN pip wheel --no-cache-dir --wheel-dir /wheels ".[all]"


FROM python:3.14-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 understudy

COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --find-links /wheels "understudy-data[all]" \
    && rm -rf /wheels

USER understudy
WORKDIR /work
ENTRYPOINT ["understudy"]
CMD ["--help"]
