# Use a slim Python image as a base.
FROM python:3.10-slim

# Install the default JRE and the wget utility.
# The wget utility is necessary to download the Spark binaries.
RUN apt-get update && \
    apt-get install -y default-jre wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Apache Spark.
# This is crucial because `pip install pyspark` only installs the Python library,
# not the full Spark binaries needed for `spark-submit`.
ARG SPARK_VERSION=4.0.1
ARG HADOOP_VERSION=3
ARG SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

RUN wget -qO /tmp/${SPARK_PACKAGE}.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" && \
    tar -xzf /tmp/${SPARK_PACKAGE}.tgz -C /opt && \
    rm /tmp/${SPARK_PACKAGE}.tgz

# Set environment variables for Java and Spark.
# We now point SPARK_HOME to the extracted directory.
ENV JAVA_HOME="/usr/lib/jvm/default-java"
ENV SPARK_HOME="/opt/${SPARK_PACKAGE}"
ENV PATH="${PATH}:${SPARK_HOME}/bin"

# Set the working directory inside the container.
WORKDIR /app

# Copy the application files into the container.
COPY . /app

# Install PySpark and any other dependencies.
RUN pip install pyspark==4.0.1

# Expose the Spark UI port.
EXPOSE 4040

# Define the command to run when the container starts.
CMD ["python", "analysis.py"]
