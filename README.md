# Fragrantica Analysis

Data Analysis of Fragrantica Dataset using both ELT and ETL

ELT
1. Initial data cleaning (clean1.py, clean2.py)
2. Import cleaned data to Supabase
3. Further data cleaning, transformation, manipulating using SQL in Supabase (datacleaning.sql)

ETL
1. Create Docker environment to run locally
2. Build ETL pipeline (pipeline.py) -> use pandas instead of sql for further data cleaning, transformation, manipulating
3. Airflow DAG successful, data loaded to pgAdmin


