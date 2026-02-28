"""
Airflow DAG: Silver Layer - Staging Models (Every 6 Hours)
Run dbt staging models for inference and resource tables (6 hour window)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'silver_layer_staging',
    default_args=default_args,
    description='Run dbt staging models every 6 hours for inference and resource tables',
    schedule_interval='0 */6 * * *',
    start_date=datetime(2026, 2, 17),
    catchup=False,
    tags=['silver', 'dbt', 'staging'],
)

DBT_PROJECT_DIR  = '/opt/dbt/resource_utilization'
DBT_PROFILES_DIR = '/opt/dbt/resource_utilization'
DB_CONN          = 'postgresql://postgres:postgres@postgres/resource_utilization'

run_dbt_staging = BashOperator(
    task_id='run_dbt_staging',
    bash_command=f"""
        echo "🔄 Running dbt staging models (6 hour window)..."
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --select stg_token_usage_proprietary \
                     stg_token_usage_open_source \
                     stg_accelerator_inventory \
                     stg_model_utilization \
                     stg_model_instance_allocation \
                     stg_customer_details \
                     stg_config_model_dimensions \
                     stg_config_model_region_availability \
                     stg_quota_default_rate_limits \
                     stg_quota_customer_rate_limit_adjustments \
                     stg_quota_customer_rate_limit_requests \
            --profiles-dir {DBT_PROFILES_DIR} \
            --project-dir {DBT_PROJECT_DIR} || exit 1
        echo "✅ dbt staging models completed"
    """,
    dag=dag,
)

run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f"""
        echo "🧪 Running dbt tests for staging models..."
        cd {DBT_PROJECT_DIR} && \
        dbt test \
            --select staging \
            --profiles-dir {DBT_PROFILES_DIR} \
            --project-dir {DBT_PROJECT_DIR} || exit 1
        echo "✅ dbt tests completed"
    """,
    dag=dag,
)

validate_staging = BashOperator(
    task_id='validate_staging',
    bash_command=f"""
        echo "🔍 Validating staging views..."
        psql {DB_CONN} -c "
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'dbt_dev_staging_silver'
            ORDER BY table_name;
        "
        echo "✅ Validation complete"
    """,
    dag=dag,
)

run_dbt_staging >> run_dbt_tests >> validate_staging
