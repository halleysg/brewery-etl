FROM apache/airflow:2.7.3

USER root

# Install Java and other dependencies
RUN apt-get update && apt-get install -y \
    procps \
    openjdk-11-jdk \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz \
    && tar -xzf hadoop-3.3.4.tar.gz -C /opt/ \
    && rm hadoop-3.3.4.tar.gz

# Switch back to airflow user
USER airflow

# Copy and install Python requirements
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt