FROM apache/airflow:2.5.0

USER root

# Install required dependencies
RUN apt-get update && apt-get install -y \
    python3-pandas \
    python3-boto3 \
    postgresql-client \
    openjdk-11-jdk-headless \
    curl && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar -xvzf spark-3.1.2-bin-hadoop3.2.tgz && \
    mv spark-3.1.2-bin-hadoop3.2 /opt/spark && \
    rm spark-3.1.2-bin-hadoop3.2.tgz

# Download PostgreSQL JDBC driver
RUN curl -L -o /opt/spark/jars/postgresql-42.2.20.jar https://jdbc.postgresql.org/download/postgresql-42.2.20.ja


# Set environment variables for Java and Spark
ENV JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-arm64

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$JAVA_HOME/bin

# Ensure the correct permissions for the Spark directory
RUN chown -R airflow /opt/spark && chmod -R 755 /opt/spark

# Switch to the airflow user before running pip install
USER airflow

# Install PySpark using pip
RUN pip install pyspark==3.1.2
RUN pip install pytest==7.4.4

# Copy your scripts and dags into the container
COPY dags /opt/airflow/dags
COPY scripts /opt/airflow/scripts
COPY tests /opt/airflow/tests



