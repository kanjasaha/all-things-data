"""
Airflow DAG: Silver Layer - Region Mapping (Manual Trigger)
Load region_mapping.csv into silver layer using dbt seed

Trigger: Manual only (run when region_mapping.csv is updated)
Schedule: None (on-demand)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'silver_layer_region_mapping',
    default_args=default_args,
    description='Load region_mapping.csv to silver layer using dbt seed (Manual Trigger)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 2, 17),
    catchup=False,
    tags=['silver', 'dbt', 'region-mapping', 'manual'],
)

# ============================================================================
# DAG TASKS
# ============================================================================

check_seed_file = BashOperator(
    task_id='check_seed_file',
    bash_command="""
        echo "🔍 Checking region_mapping.csv seed file..."
        echo ""
        if [ -f /opt/dbt/my_project/seeds/region_mapping.csv ]; then
            echo "✅ region_mapping.csv found"
            ls -lh /opt/dbt/my_project/seeds/region_mapping.csv
            echo ""
            echo "📊 Row count:"
            wc -l < /opt/dbt/my_project/seeds/region_mapping.csv
        else
            echo "❌ region_mapping.csv NOT FOUND at /opt/dbt/my_project/seeds/"
            exit 1
        fi
        echo ""
        echo "✅ Seed file check passed"
    """,
    dag=dag,
)

run_dbt_seed = BashOperator(
    task_id='run_dbt_seed',
    bash_command="""
        echo "🌱 Running dbt seed for region_mapping..."
        echo ""
        cd /opt/dbt/my_project && \
        dbt seed \
            --select region_mapping \
            --profiles-dir /opt/dbt/my_project \
            --project-dir /opt/dbt/my_project \
            --full-refresh
        echo ""
        echo "✅ dbt seed completed"
    """,
    dag=dag,
)

validate_seed = BashOperator(
    task_id='validate_seed',
    bash_command="""
        echo "🔍 Validating region_mapping table in silver layer..."
        echo ""
        ROW_COUNT=$(psql postgresql://postgres:postgres@postgres/resource_utilization \
            -t -c "SELECT COUNT(*) FROM staging_silver.region_mapping;")
        echo "📊 Rows in staging_silver.region_mapping: $ROW_COUNT"
        echo ""
        if [ "$ROW_COUNT" -gt "0" ]; then
            echo "✅ Validation passed - table has data"
        else
            echo "❌ Validation failed - table is empty"
            exit 1
        fi
    """,
    dag=dag,
)

# ============================================================================
# DAG DEPENDENCIES
# ============================================================================

check_seed_file >> run_dbt_seed >> validate_seed
