"""
Airflow DAG: Seed Layer - Region Mapping (Manual Trigger)
Load region_mapping.csv into seed layer using dbt seed

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
    'seed_layer_region_mapping',
    default_args=default_args,
    description='Load region_mapping.csv to seed layer using dbt seed (Manual Trigger)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 2, 17),
    catchup=False,
    tags=['seed', 'dbt', 'region-mapping', 'manual'],
)

# ============================================================================
# PATH CONSTANTS
# ============================================================================

DBT_PROJECT_DIR  = '/opt/dbt/resource_utilization'
DBT_PROFILES_DIR = '/opt/dbt/resource_utilization'
DB_CONN          = 'postgresql://postgres:postgres@postgres/resource_utilization'
SEED_TABLE       = 'dbt_dev_seeds.region_mapping'

# ============================================================================
# DAG TASKS
# ============================================================================

check_seed_file = BashOperator(
    task_id='check_seed_file',
    bash_command=f"""
        echo "🔍 Checking region_mapping.csv seed file..."
        echo ""
        if [ -f {DBT_PROJECT_DIR}/seeds/region_mapping.csv ]; then
            echo "✅ region_mapping.csv found"
            ls -lh {DBT_PROJECT_DIR}/seeds/region_mapping.csv
            echo ""
            echo "📊 Row count (excluding header):"
            tail -n +2 {DBT_PROJECT_DIR}/seeds/region_mapping.csv | wc -l
        else
            echo "❌ region_mapping.csv NOT FOUND at {DBT_PROJECT_DIR}/seeds/"
            exit 1
        fi
        echo ""
        echo "✅ Seed file check passed"
    """,
    dag=dag,
)

run_dbt_seed = BashOperator(
    task_id='run_dbt_seed',
    bash_command=f"""
        echo "🌱 Running dbt seed for region_mapping..."
        echo ""
        cd {DBT_PROJECT_DIR} && \
        dbt seed \
            --select region_mapping \
            --profiles-dir {DBT_PROFILES_DIR} \
            --project-dir {DBT_PROJECT_DIR} \
            --full-refresh || exit 1
        echo ""
        echo "✅ dbt seed completed"
        echo ""
        echo "📊 Verifying seed table in database:"
        psql {DB_CONN} -t -c "SELECT schemaname, tablename FROM pg_tables WHERE tablename = 'region_mapping';"
    """,
    dag=dag,
)

validate_seed = BashOperator(
    task_id='validate_seed',
    bash_command=f"""
        echo "🔍 Validating region_mapping seed table..."
        echo ""

        SEED_COUNT=$(psql {DB_CONN} \
            -t -c "SELECT COUNT(*) FROM {SEED_TABLE};" | xargs)
        echo "📊 Rows in {SEED_TABLE}: $SEED_COUNT"

        echo ""
        if [ "$SEED_COUNT" -gt "0" ]; then
            echo "✅ Validation passed - region_mapping has data"
        else
            echo "❌ Validation failed - region_mapping is empty"
            exit 1
        fi
    """,
    dag=dag,
)

# ============================================================================
# DAG DEPENDENCIES
# ============================================================================

check_seed_file >> run_dbt_seed >> validate_seed
