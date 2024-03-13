# Use an official Apache Spark runtime as the base image
FROM python:3-slim

USER root

RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean;

# Set the working directory to /app
WORKDIR /app

# Copy the local Python script and requirements file to the container at /app
COPY /check /app/check
COPY /ingestion /app/ingestion
COPY /transformation /app/transformation
COPY /utils /app/utils
COPY main.py /app/main.py
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip3 install --no-cache-dir -r requirements.txt

# Set the Spark application's entry point for PySpark
CMD ["python3", "main.py"]
