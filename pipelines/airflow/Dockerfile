FROM apache/airflow:2.9.2-python3.11

# Copy requirements
COPY pipelines/airflow/requirements.txt /requirements.txt

# Install requirements
RUN pip install --no-cache-dir -r /requirements.txt
