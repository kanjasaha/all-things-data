"""
Airflow DAG: Silver Layer - Staging Daily Models (Every 24 Hours)
Run dbt staging models for daily revenue table (25 hour window)
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
    'silver_layer_staging_daily',
    default_args=default_args,
    description='Run dbt staging models every 24 hours for daily revenue table',
    schedule_interval='0 1 * * *',  # 1am daily
    start_date=datetime(2026, 2, 17),
    catchup=False,
    tags=['silver', 'dbt', 'staging', 'daily'],
)

DBT_PROJECT_DIR  = '/opt/dbt/resource_utilization'
DBT_PROFILES_DIR = '/opt/dbt/resource_utilization'
DB_CONN          = 'postgresql://postgres:postgres@postgres/resource_utilization'

run_dbt_staging_daily = BashOperator(
    task_id='run_dbt_staging_daily',
    bash_command=f"""
        echo "🔄 Running dbt daily staging model (25 hour window)..."
        cd {DBT_PROJECT_DIR} && \
        dbt run \
            --select stg_revenue_account_daily \
            --profiles-dir {DBT_PROFILES_DIR} \
            --project-dir {DBT_PROJECT_DIR} || exit 1
        echo "✅ dbt daily staging model completed"
    """,
    dag=dag,
)

run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f"""
        echo "🧪 Running dbt tests for revenue staging model..."
        cd {DBT_PROJECT_DIR} && \
        dbt test \
            --select stg_revenue_account_daily \
            --profiles-dir {DBT_PROFILES_DIR} \
            --project-dir {DBT_PROJECT_DIR} || exit 1
        echo "✅ dbt tests completed"
    """,
    dag=dag,
)

validate_staging = BashOperator(
    task_id='validate_staging',
    bash_command=f"""
        echo "🔍 Validating revenue staging view..."
        psql {DB_CONN} -c "
            SELECT COUNT(*), MIN(revenue_date), MAX(revenue_date)
            FROM dbt_dev_staging_silver.stg_revenue_account_daily;
        "
        echo "✅ Validation complete"
    """,
    dag=dag,
)

run_dbt_staging_daily >> run_dbt_tests >> validate_staging
