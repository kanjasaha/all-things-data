FROM apache/airflow:2.5.3

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install Astronomer Cosmos for dbt integration
# Install dbt-core and dbt-postgres
RUN pip install --no-cache-dir \
    astronomer-cosmos[dbt-postgres]==1.0.0 \
    dbt-core==1.4.6 \
    dbt-postgres==1.4.6

# Set environment variables for dbt
ENV DBT_PROFILES_DIR=/opt/dbt
ENV DBT_PROJECT_DIR=/opt/dbt
