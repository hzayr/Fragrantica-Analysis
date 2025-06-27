# Fragrantica-Analysis

1. At first glance, complex changes have to be made (clean1.py):
Name Column - "for" stuck to previous word
Description Column - "by" stuck to previous and after word & "is" stuck to previous word

2. Removed null values from Name column and added id column for all rows as primary key

3a. Imported to supabase - further data cleaning (datacleaning.sql)

3b. Set up Docker, added ETL pipeline (/dags/pipeline.py) to load to pgAdmin database using Apache Airflow
