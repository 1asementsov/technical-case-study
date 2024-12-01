# Use an official Apache Spark image with PySpark
FROM apache/spark-py:latest

# Set working directory inside the container
WORKDIR /src

# Copy the current directory contents into the container
COPY . .

# Ensure that we are using root user to fix permissions
USER root

# Fix permissions for /.cache/pip and /.local directories
RUN mkdir -p /root/.cache/pip /root/.local && \
    chmod -R 777 /root/.cache /root/.local

# Upgrade pip and install Python dependencies from requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Set environment variables for Spark and Java
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Fix permissions for Spark directories and files
RUN chown -R root:root /opt/spark && \
    chmod -R 777 /opt/spark

# Define the default command to run main.py first, then output_check.py
CMD ["bash", "-c", "spark-submit main.py && spark-submit output_check.py"]