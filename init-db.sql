-- Create Metabase database
CREATE DATABASE metabase;

-- Create Airflow database  
CREATE DATABASE airflow;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE metabase TO postgres;
GRANT ALL PRIVILEGES ON DATABASE airflow TO postgres;
