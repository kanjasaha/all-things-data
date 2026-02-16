# Modern Data Stack - Docker Compose Setup

Complete data engineering environment with PostgreSQL, Airflow, dbt, Airbyte, Metabase, JupyterLab, and pgAdmin.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Modern Data Stack                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Data Sources → Airbyte → PostgreSQL → dbt → Metabase/Jupyter   │
│                              ↑                                    │
│                          Airflow (Orchestration)                 │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

## Components

| Service | Port | Credentials | Description |
|---------|------|-------------|-------------|
| **PostgreSQL** | 5432 | postgres/postgres | Primary database |
| **pgAdmin** | 5050 | admin@admin.com/admin | PostgreSQL admin UI |
| **Airflow** | 8080 | airflow/airflow | Workflow orchestration (with Cosmos) |
| **Metabase** | 3000 | (setup on first login) | Business intelligence |
| **JupyterLab** | 8888 | (no password) | Data science notebooks |
| **Airbyte** | 8000 | (setup on first login) | Data integration |
| **dbt** | - | - | Data transformation (CLI + Cosmos) |

## Prerequisites

- Docker Desktop installed and running
- At least 12GB RAM allocated to Docker (increased for Airbyte)
- 30GB free disk space

## Quick Start

### 1. Setup Directory Structure

```bash
mkdir -p ~/data-stack
cd ~/data-stack

# Create required directories
mkdir -p airflow/dags airflow/logs airflow/plugins
mkdir -p dbt
mkdir -p jupyter-notebooks
mkdir -p airbyte/temporal

# Copy files to appropriate locations
# - docker-compose.yml → ~/data-stack/
# - airflow-Dockerfile → ~/data-stack/airflow/Dockerfile
# - dbt-Dockerfile → ~/data-stack/dbt/Dockerfile
# - sample_dbt_dag.py → ~/data-stack/airflow/dags/ (optional)

# Create Temporal config for Airbyte
cat > airbyte/temporal/development.yaml << EOF
{}
EOF
```

### 2. Start All Services

```bash
# Start all services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f
```

### 3. Initialize Database

```bash
# Connect to PostgreSQL and create schemas
docker exec -it postgres14 psql -U postgres -d resource_utilization

# Then run:
CREATE SCHEMA IF NOT EXISTS raw_bronze;
CREATE SCHEMA IF NOT EXISTS staging_silver;
CREATE SCHEMA IF NOT EXISTS mart_gold;
\q
```

### 4. Access Services

- **pgAdmin**: http://localhost:5050
- **Airflow**: http://localhost:8080
- **Metabase**: http://localhost:3000
- **JupyterLab**: http://localhost:8888

## Service-Specific Setup

### pgAdmin Configuration

1. Login at http://localhost:5050 (admin@admin.com/admin)
2. Add Server:
   - Name: `postgres14`
   - Host: `postgres` (use service name, not localhost)
   - Port: `5432`
   - Username: `postgres`
   - Password: `postgres`

### Airflow Configuration

Airflow will auto-initialize on first run. Access at http://localhost:8080

Default DAGs location: `./airflow/dags/`

### dbt Usage

```bash
# Initialize dbt project
docker-compose run --rm dbt init my_project

# Run dbt models
docker-compose run --rm dbt run

# Test models
docker-compose run --rm dbt test

# Generate docs
docker-compose run --rm dbt docs generate
```

### Airbyte Setup

Airbyte is now included in the main docker-compose! Access at http://localhost:8000

On first access:
1. Create your account (any email works)
2. Set up connections to ingest data
3. Configure source (e.g., File/HTTP for JSON files)
4. Configure destination (PostgreSQL - use host: `postgres`)

Connection settings for PostgreSQL destination:
- Host: `postgres`
- Port: `5432`
- Database: `resource_utilization`
- Username: `postgres`
- Password: `postgres`
- Schema: `raw_bronze`

### Astronomer Cosmos (dbt + Airflow Integration)

Cosmos is pre-installed in Airflow! It automatically turns dbt models into Airflow tasks.

**Benefits:**
- Each dbt model becomes an Airflow task
- Task dependencies mirror dbt model dependencies
- Monitor dbt runs in Airflow UI
- Retry failed dbt models individually

**Sample DAG** (already in `airflow/dags/sample_dbt_dag.py`):

```python
from cosmos import DbtDag, ProjectConfig, ProfileConfig

dbt_dag = DbtDag(
    dag_id="dbt_resource_utilization",
    project_config=ProjectConfig(dbt_project_path="/opt/dbt/my_project"),
    profile_config=ProfileConfig(...),
    schedule_interval="0 2 * * *",
)
```

The DAG will automatically:
- Run `dbt deps`
- Execute each model as a separate task
- Respect model dependencies
- Run tests (if configured)

### JupyterLab

Access at http://localhost:8888 (no password required)

Notebooks saved in `./jupyter-notebooks/` directory.

## Database Schema

The `resource_utilization` database has three schemas:

- **raw_bronze**: Raw data from sources (config files, DynamoDB, clickstream)
- **staging_silver**: Cleaned and transformed data
- **mart_gold**: Business-ready analytics tables

## Data Flow

1. **Ingestion**: Airbyte loads data from sources → PostgreSQL (raw_bronze)
2. **Transformation**: dbt transforms data (raw_bronze → staging_silver → mart_gold)
3. **Orchestration**: Airflow + Cosmos schedules dbt models as individual tasks
4. **Analysis**: Metabase/JupyterLab for visualization and exploration
5. **Management**: pgAdmin for database administration

## Astronomer Cosmos Benefits

Cosmos bridges dbt and Airflow:
- **Automatic DAG generation** from dbt projects
- **Task-level granularity** - each model is a task
- **Native dependencies** - Airflow respects dbt dependencies
- **Better monitoring** - see each model's status in Airflow UI
- **Selective reruns** - retry individual models without re-running entire dbt project

## Common Commands

### Start/Stop Services

```bash
# Start all
docker-compose up -d

# Stop all
docker-compose down

# Restart specific service
docker-compose restart airflow-webserver

# View logs
docker-compose logs -f <service-name>
```

### Database Operations

```bash
# Connect to PostgreSQL
docker exec -it postgres14 psql -U postgres -d resource_utilization

# Backup database
docker exec postgres14 pg_dump -U postgres resource_utilization > backup.sql

# Restore database
docker exec -i postgres14 psql -U postgres resource_utilization < backup.sql
```

### Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove everything including images
docker-compose down -v --rmi all
```

## Troubleshooting

### Airflow won't start
- Check PostgreSQL is running: `docker-compose ps postgres`
- View logs: `docker-compose logs airflow-webserver`
- Reinitialize: `docker-compose run --rm airflow-init`

### Out of memory
- Increase Docker memory: Docker Desktop → Settings → Resources
- Recommended: 8GB+ RAM

### Port already in use
- Check what's using the port: `lsof -i :8080`
- Change port in docker-compose.yml

### Cannot connect to PostgreSQL from host
- Use `localhost:5432` from host machine
- Use `postgres:5432` from within Docker network

## File Locations

```
~/data-stack/
├── docker-compose.yml
├── airflow/
│   ├── Dockerfile         # Custom Airflow with Cosmos
│   ├── dags/             # Airflow DAG files (including Cosmos DAGs)
│   ├── logs/             # Airflow logs
│   └── plugins/          # Airflow plugins
├── dbt/
│   ├── Dockerfile        # dbt image
│   └── my_project/       # dbt project (created via dbt init)
├── jupyter-notebooks/    # Jupyter notebooks
└── airbyte/
    └── temporal/         # Airbyte Temporal config
```

## Environment Variables

Key environment variables (already set in docker-compose.yml):

- `POSTGRES_USER`: postgres
- `POSTGRES_PASSWORD`: postgres
- `POSTGRES_DB`: resource_utilization
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: PostgreSQL connection string

## Security Notes

⚠️ **This setup is for DEVELOPMENT only**

For production:
- Change all default passwords
- Use secrets management
- Enable SSL/TLS
- Configure proper authentication
- Set up network security
- Enable audit logging

## Next Steps

1. Load your JSON/CSV data into PostgreSQL using Airbyte
2. Create dbt models in `dbt/models/`
3. Build Airflow DAGs in `airflow/dags/`
4. Create dashboards in Metabase
5. Develop analyses in JupyterLab

## Support

- PostgreSQL: https://www.postgresql.org/docs/
- Airflow: https://airflow.apache.org/docs/
- dbt: https://docs.getdbt.com/
- Airbyte: https://docs.airbyte.com/
- Metabase: https://www.metabase.com/docs/
- JupyterLab: https://jupyterlab.readthedocs.io/
