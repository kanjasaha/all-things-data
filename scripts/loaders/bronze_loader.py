#!/usr/bin/env python3
"""
Bronze Layer Auto-Loader
Watches config-files/ directory and auto-loads JSON files to PostgreSQL
"""

import json
import time
import os
import sys
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ============================================================================
# CONFIGURATION
# ============================================================================

# PostgreSQL connection
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'resource_utilization'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}

# File paths
CONFIG_DIR = Path('/app/config-files')  # Inside Docker
SOURCE_FILES = {
    'model_configuration.json': 'raw_bronze.model_configuration',
    'model_region_availability.json': 'raw_bronze.model_region_availability'
}

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================

def get_db_connection():
    """Create PostgreSQL connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        sys.exit(1)


def load_model_configuration(filepath):
    """Load model_configuration.json into bronze table"""
    print(f"📂 Loading {filepath}...")
    
    try:
        # Read JSON
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        models = data.get('models', [])
        if not models:
            print("⚠️  No models found in JSON")
            return 0
        
        # Prepare data for insertion
        snapshot_date = datetime.now().date()
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
        
        # Insert into database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO raw_bronze.model_configuration (
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
        return inserted
        
    except Exception as e:
        print(f"❌ Error loading model_configuration: {e}")
        return 0


def load_model_region_availability(filepath):
    """Load model_region_availability.json into bronze table"""
    print(f"📂 Loading {filepath}...")
    
    try:
        # Read JSON
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        mappings = data.get('model_region_availability', [])
        if not mappings:
            print("⚠️  No mappings found in JSON")
            return 0
        
        # Prepare data for insertion
        snapshot_date = datetime.now().date()
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
        
        # Insert into database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO raw_bronze.model_region_availability (
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
        return inserted
        
    except Exception as e:
        print(f"❌ Error loading model_region_availability: {e}")
        return 0


def load_file(filepath):
    """Route file to appropriate loader"""
    filename = filepath.name
    
    if filename == 'model_configuration.json':
        return load_model_configuration(filepath)
    elif filename == 'model_region_availability.json':
        return load_model_region_availability(filepath)
    else:
        print(f"⚠️  Unknown file: {filename}")
        return 0


# ============================================================================
# FILE WATCHER
# ============================================================================

class ConfigFileHandler(FileSystemEventHandler):
    """Handle file system events"""
    
    def on_modified(self, event):
        if event.is_directory:
            return
        
        filepath = Path(event.src_path)
        if filepath.name in SOURCE_FILES:
            print(f"\n🔔 File changed: {filepath.name}")
            time.sleep(1)  # Wait for file write to complete
            load_file(filepath)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    print("="*80)
    print("🚀 Bronze Layer Auto-Loader")
    print("="*80)
    print(f"Watching directory: {CONFIG_DIR}")
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"Files to watch: {list(SOURCE_FILES.keys())}")
    print("="*80)
    
    # Initial load
    print("\n📥 Initial load...")
    for filename in SOURCE_FILES.keys():
        filepath = CONFIG_DIR / filename
        if filepath.exists():
            load_file(filepath)
        else:
            print(f"⚠️  File not found: {filename}")
    
    # Start file watcher
    print("\n👀 Starting file watcher...")
    print("Press Ctrl+C to stop\n")
    
    event_handler = ConfigFileHandler()
    observer = Observer()
    observer.schedule(event_handler, str(CONFIG_DIR), recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping file watcher...")
        observer.stop()
    
    observer.join()
    print("✅ Stopped")


if __name__ == "__main__":
    main()
