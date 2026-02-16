FROM python:3.10-slim

# Install git and dbt
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir \
    dbt-core==1.4.6 \
    dbt-postgres==1.4.6

WORKDIR /usr/app

ENTRYPOINT ["dbt"]
