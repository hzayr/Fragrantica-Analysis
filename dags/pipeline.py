
# DAG: Directed Acyclic Graph - a collection of tasks that run in a defined order.
# GOAL:
# Build a data pipeline using Airflow that:
# 1. Extracts fragrance data from fra_perfumes.csv
# 2. Transforms/cleans the data (similar to clean1.py and clean2.py)
# 3. Loads it into a PostgreSQL database

# This DAG runs locally with Docker and uses open-source tools only.
# Extract: Pull data from CSV file (fra_perfumes.csv)
# Transform: Clean/process the data (fix spacing, remove nulls, add ID)
# Load: Insert data into target system (PostgreSQL)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import List, Dict, Any
import pandas as pd
import os

#==============================
# Step 1: DAG Configuration
#==============================

# Default arguments for all tasks
default_args = {
'owner': 'airflow',
'depends_on_past': False,
'start_date': datetime (2024, 6, 20), 
'retries': 1,
'retry_delay': timedelta (minutes=5)
}

# Create the DAG object
dag = DAG(
    'fragrantica_etl',              # DAG ID
    default_args=default_args,
    description='ETL pipeline for Fragrantica perfume data',
    schedule='@daily',              # Run once a day
    catchup=False,                  # Do not run for past dates
    tags=['etl', 'fragrantica', 'postgres'],
)

#=======================================================
# Step 2: EXTRACT - Fetch data from fra_perfumes.csv
#=======================================================

def extract_perfume_data(**kwargs):
    """
    EXTRACT PHASE: Pull raw perfume data from fra_perfumes.csv file.
    Returns all perfume records from the CSV file.
    """
    print("EXTRACT PHASE: Reading perfume data from fra_perfumes.csv...")

    try:
        # Get the path to the CSV file (in the same directory as the DAG file)
        csv_path = os.path.join(os.path.dirname(__file__), 'fra_perfumes.csv')
        
        print(f"Attempting to read CSV from: {csv_path}")
        
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Convert DataFrame to list of dictionaries
        raw_perfumes = df.to_dict('records')

        # Push the raw perfume data to Xcom for the next task
        kwargs['ti'].xcom_push(key='raw_perfumes', value=raw_perfumes)

        print(f"EXTRACT COMPLETE: Retrieved {len(raw_perfumes)} raw perfume records.")
        return "Extract completed successfully."
    
    except Exception as e:
        print(f"Error in EXTRACT phase: {e}")
        kwargs['ti'].xcom_push(key='extract_error', value=str(e))
        raise
    
# Create the Extract task using PythonOperator
extract_task = PythonOperator(
    task_id='extract_perfume_data',
    python_callable=extract_perfume_data,
    dag=dag,
)

# ===============================================
# Step 3: TRANSFORM - Clean and process the data
# ===============================================

def transform_perfume_data(**kwargs):
    """
    TRANSFORM PHASE: Clean and process the raw perfume data.
    - Fix spacing issues in Name and Description (similar to clean1.py)
    - Remove null values in Name column
    - Add ID column as primary key (similar to clean2.py)
    """
    ti = kwargs['ti']
    raw_perfumes = ti.xcom_pull(task_ids='extract_perfume_data', key='raw_perfumes') or []

    print(f"TRANSFORM PHASE: Processing {len(raw_perfumes)} raw records...")

    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(raw_perfumes)
    
    # Apply transformations similar to clean1.py
    # Fix spacing in Name column - add space before "for" if it's stuck to previous word
    df['Name'] = df['Name'].str.replace(r'(?<=[a-zA-Z])for', ' for', regex=True)
    
    # Fix spacing in Description column - add space before and after "by" if stuck
    df['Description'] = df['Description'].str.replace(r'(?<!\s)by(?!\s)', ' by ', regex=True)
    
    # Add space before "is" if stuck to previous word
    df['Description'] = df['Description'].str.replace(r'(?<=[a-zA-Z])(?<!\b)is(?!\w)', ' is', regex=True)
    
    # Apply transformations similar to clean2.py
    # Remove null values in Name column
    df = df.dropna(subset=['Name'])
    
    # Add ID column as primary key
    df['id'] = range(1, len(df) + 1)
    
    # Reorder columns to put ID first
    cols = ['id'] + [col for col in df.columns if col != 'id']
    df = df[cols]
    
    # Convert back to list of dictionaries
    transformed_data = df.to_dict('records')

    ti.xcom_push(key='transformed_perfumes', value=transformed_data)
    print(f"TRANSFORM COMPLETE: Processed {len(transformed_data)} cleaned records.")
    return "Transform completed successfully."

# Create the Transform task using PythonOperator
transform_task = PythonOperator(
    task_id='transform_perfume_data',
    python_callable=transform_perfume_data,
    dag=dag,
)

# ==========================================
# Step 4: LOAD - Insert data into PostgreSQL
# ==========================================

def load_perfume_data(**kwargs):
    """
    LOAD PHASE: Create a table if needed, then insert transformed perfume data into PostgreSQL.
    Uses the schema: id, Name, Gender, Rating Value, Rating Count, Main Accords, Perfumers, Description, url
    """
    print("LOAD PHASE: Inserting perfume data into PostgreSQL...")

    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_perfume_data', key='transformed_perfumes') 

    if not transformed_data:
        print("LOAD ERROR: No transformed data available to load.")
        return "No data to load."
    
    postgres_hook = PostgresHook(postgres_conn_id='perfumes_connection')

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS perfumes (
        id INTEGER PRIMARY KEY,
        name VARCHAR(500),
        gender VARCHAR(100),
        rating_value DECIMAL(3,2),
        rating_count INTEGER,
        main_accords TEXT,
        perfumers TEXT,
        description TEXT,
        url VARCHAR(1000)
    );
    """
    postgres_hook.run(create_table_query)

    insert_query = """
    INSERT INTO perfumes (id, name, gender, rating_value, rating_count, main_accords, perfumers, description, url)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        gender = EXCLUDED.gender,
        rating_value = EXCLUDED.rating_value,
        rating_count = EXCLUDED.rating_count,
        main_accords = EXCLUDED.main_accords,
        perfumers = EXCLUDED.perfumers,
        description = EXCLUDED.description,
        url = EXCLUDED.url;
    """

    try:
        # Prepare batch data for insertion
        batch_data = []
        for perfume in transformed_data:
            # Handle rating count - remove commas and convert to integer
            rating_count = perfume.get('Rating Count', 0)
            if isinstance(rating_count, str):
                rating_count = rating_count.replace(',', '') if rating_count else '0'
                rating_count = int(rating_count) if rating_count.isdigit() else 0
            
            # Handle rating value - convert to float
            rating_value = perfume.get('Rating Value')
            if rating_value == '' or rating_value is None:
                rating_value = None
            else:
                try:
                    rating_value = float(rating_value)
                except (ValueError, TypeError):
                    rating_value = None

            batch_data.append((
                perfume.get('id'),
                perfume.get('Name'),
                perfume.get('Gender'),
                rating_value,
                rating_count,
                str(perfume.get('Main Accords', '')),
                str(perfume.get('Perfumers', '')),
                perfume.get('Description'),
                perfume.get('url')
            ))

        # Execute batch insert using get_conn() for direct access to cursor
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        
        # Use executemany for efficient batch insertion
        cursor.executemany("""
            INSERT INTO perfumes (id, name, gender, rating_value, rating_count, main_accords, perfumers, description, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                gender = EXCLUDED.gender,
                rating_value = EXCLUDED.rating_value,
                rating_count = EXCLUDED.rating_count,
                main_accords = EXCLUDED.main_accords,
                perfumers = EXCLUDED.perfumers,
                description = EXCLUDED.description,
                url = EXCLUDED.url
        """, batch_data)
        
        conn.commit()
        cursor.close()
        conn.close()

        print(f"LOAD COMPLETE: Loaded {len(transformed_data)} records in batch.")
        return f"Loaded {len(transformed_data)} perfumes."
    
    except Exception as e:
        print(f"LOAD ERROR: {e}")
        raise


# Create the Load task using PythonOperator
load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_perfume_data,
    dag=dag,
)

# ==========================================
# Step 5: ETL Task Dependencies
# ==========================================

# Set task dependencies to create a clear ETL flow:
# Extract must run before Transform
extract_task >> transform_task >> load_task
# test