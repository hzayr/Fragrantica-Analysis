FROM apache/airflow:3.0.1

# Install additional requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
