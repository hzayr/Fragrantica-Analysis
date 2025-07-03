FROM apache/airflow:3.0.0

# Install Postgres provider and psycopg2
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# test