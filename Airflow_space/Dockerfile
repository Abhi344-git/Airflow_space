FROM apache/airflow:2.6.3-python3.11

# Install any additional Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || true

# Set working directory
WORKDIR /opt/airflow
