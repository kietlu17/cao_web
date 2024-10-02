# Base image for PySpark
FROM openjdk:11-jre-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Set Python3 as default python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Set the working directory
WORKDIR /app

# Copy requirements.txt to the container
COPY requirements.txt /app/

# Install Python dependencies
RUN pip install -r requirements.txt

# Set up environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download and set up Spark
RUN curl -L -o spark.tgz https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    mkdir -p /opt && \
    tar -xzf spark.tgz -C /opt && \
    mv /opt/spark-3.4.0-bin-hadoop3 /opt/spark && \
    rm spark.tgz

# Add MongoDB Spark Connector jar file
RUN curl -L -o /opt/spark/jars/mongo-spark-connector_2.12-10.1.1.jar https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.1.1/mongo-spark-connector_2.12-10.1.1.jar

# Copy the Spark script to the container
COPY spark_mongo.py /app/

# Run the PySpark script
CMD ["spark-submit", "sam.py"]
