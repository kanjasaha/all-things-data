"""
Airflow DAG: Bronze Layer Data Ingestion (Manual Trigger)
Load model configuration and region availability data from JSON files

Trigger: Manual only (run when config files are updated)
Schedule: None (on-demand)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

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
    'bronze_layer_ingestion_manual',
    default_args=default_args,
    description='Load model config and region availability data to bronze layer (Manual Trigger)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 2, 17),
    catchup=False,
    tags=['bronze', 'ingestion', 'model-config', 'manual'],
)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

DB_CONFIG = {
    'host': 'postgres',
    'port': '5432',
    'database': 'resource_utilization',
    'user': 'postgres',
    'password': 'postgres'
}

CONFIG_DIR = Path('/opt/airflow/config-files')

# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def load_model_configuration(**context):
    """Load model configuration JSON to bronze table"""

    filepath = CONFIG_DIR / 'model_configuration.json'

    print(f"📂 Reading file: {filepath}")

    with open(filepath, 'r') as f:
        data = json.load(f)

    models = data.get('models', [])
    snapshot_date = datetime.now().date()

    print(f"📊 Found {len(models)} models in JSON")

    records = []
    for model in models:
        record = (
            model['publisher_name'],
            model['model_display_name'],
            model['model_resource_name'],
            model['model_family'],
            model['model_variant'],
            model['model_version'],
            model['model_task'],
            model['inference_scope'],
            model['is_open_source'],
            model['replicas'],
            model['max_concurrency'],
            model['ideal_concurrency'],
            model['max_rps'],
            model['accelerator_type'],
            model['accelerators_per_replica'],
            model['memory_gb'],
            model['endpoint'],
            model['tokens_per_second'],
            model['avg_tokens_per_request'],
            model['avg_latency_seconds'],
            snapshot_date,
            filepath.name
        )
        records.append(record)

    print(f"💾 Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO raw_bronze.config_model_dimensions (
            publisher_name, model_display_name, model_resource_name,
            model_family, model_variant, model_version,
            model_task, inference_scope, is_open_source,
            replicas, max_concurrency, ideal_concurrency, max_rps,
            accelerator_type, accelerators_per_replica, memory_gb,
            endpoint, tokens_per_second, avg_tokens_per_request,
            avg_latency_seconds, snapshot_date, source_file
        ) VALUES %s
        ON CONFLICT (model_variant, snapshot_date) DO NOTHING
    """

    execute_values(cursor, insert_query, records)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {inserted} model configuration records")
    context['task_instance'].xcom_push(key='model_config_count', value=inserted)

    return inserted


def load_model_region_availability(**context):
    """Load model region availability JSON to bronze table"""

    filepath = CONFIG_DIR / 'model_region_availability.json'

    print(f"📂 Reading file: {filepath}")

    with open(filepath, 'r') as f:
        data = json.load(f)

    mappings = data.get('model_region_availability', [])
    snapshot_date = datetime.now().date()

    print(f"📊 Found {len(mappings)} region mappings in JSON")

    records = []
    for mapping in mappings:
        record = (
            mapping['model_variant'],
            mapping['source_region'],
            mapping['deployed_at'],
            mapping['is_active'],
            mapping.get('snapshot_date', snapshot_date),
            filepath.name
        )
        records.append(record)

    print(f"💾 Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO raw_bronze.config_model_region_availability (
            model_variant, source_region, deployed_at,
            is_active, snapshot_date, source_file
        ) VALUES %s
        ON CONFLICT (model_variant, source_region, snapshot_date) DO NOTHING
    """

    execute_values(cursor, insert_query, records)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Inserted {inserted} region availability records")
    context['task_instance'].xcom_push(key='region_availability_count', value=inserted)

    return inserted


def validate_bronze_data(**context):
    """Validate data quality in bronze layer"""

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    issues = []

    # Check 1: All models have region availability
    cursor.execute("""
        SELECT cmd.model_variant
        FROM raw_bronze.config_model_dimensions cmd
        WHERE snapshot_date = CURRENT_DATE
          AND NOT EXISTS (
              SELECT 1
              FROM raw_bronze.config_model_region_availability mra
              WHERE mra.model_variant = cmd.model_variant
                AND mra.snapshot_date = CURRENT_DATE
          )
    """)

    orphan_models = cursor.fetchall()
    if orphan_models:
        issues.append(f"⚠️  {len(orphan_models)} models without region availability: {[m[0] for m in orphan_models]}")

    # Check 2: Count records loaded today
    cursor.execute("""
        SELECT COUNT(*) FROM raw_bronze.config_model_dimensions
        WHERE snapshot_date = CURRENT_DATE
    """)
    config_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) FROM raw_bronze.config_model_region_availability
        WHERE snapshot_date = CURRENT_DATE
    """)
    region_count = cursor.fetchone()[0]

    # Check 3: Count total records (all time)
    cursor.execute("SELECT COUNT(*) FROM raw_bronze.config_model_dimensions")
    total_config = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM raw_bronze.config_model_region_availability")
    total_region = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    print("=" * 70)
    print("📊 Bronze Layer Validation")
    print("=" * 70)
    print(f"Model configurations loaded today:    {config_count}")
    print(f"Region availabilities loaded today:   {region_count}")
    print(f"Total model configurations (all time): {total_config}")
    print(f"Total region availabilities (all time): {total_region}")

    if issues:
        print("\n⚠️  Data Quality Issues:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("\n✅ All validation checks passed!")

    print("=" * 70)

    return len(issues) == 0


# ============================================================================
# DAG TASKS
# ============================================================================

check_files = BashOperator(
    task_id='check_source_files',
    bash_command="""
        echo "🔍 Checking source files..."
        echo ""
        if [ -f /opt/airflow/config-files/model_configuration.json ]; then
            echo "✅ model_configuration.json found"
            ls -lh /opt/airflow/config-files/model_configuration.json
        else
            echo "❌ model_configuration.json NOT FOUND"
            exit 1
        fi
        echo ""
        if [ -f /opt/airflow/config-files/model_region_availability.json ]; then
            echo "✅ model_region_availability.json found"
            ls -lh /opt/airflow/config-files/model_region_availability.json
        else
            echo "❌ model_region_availability.json NOT FOUND"
            exit 1
        fi
        echo ""
        echo "✅ All source files found"
    """,
    dag=dag,
)

load_config = PythonOperator(
    task_id='load_model_configuration',
    python_callable=load_model_configuration,
    provide_context=True,
    dag=dag,
)

load_regions = PythonOperator(
    task_id='load_region_availability',
    python_callable=load_model_region_availability,
    provide_context=True,
    dag=dag,
)

validate = PythonOperator(
    task_id='validate_bronze_data',
    python_callable=validate_bronze_data,
    provide_context=True,
    dag=dag,
)

# ============================================================================
# DAG DEPENDENCIES
# ============================================================================

check_files >> [load_config, load_regions] >> validate
