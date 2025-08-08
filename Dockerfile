# FROM bitnami/spark:latest

# USER root

# RUN apt-get update && apt-get install -y python3-pip && \
#     pip3 install cassandra-driver && \
#     apt-get clean && rm -rf /var/lib/apt/lists/*

# USER 1001

#downgrade ke 3.5.1
FROM bitnami/spark:3.5.1

USER root

# Install pip dan driver Cassandra
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install cassandra-driver && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy custom JARs ke folder terpisah
# COPY /home/hadi/.ivy2.5.2/jars /opt/bitnami/spark/jars_custom
# COPY jars /opt/bitnami/spark/jars_custom
#beri ownership ke user 1001 agar tidak hanya dimiliki root
# RUN chown -R 1001:1001 /opt/bitnami/spark/jars_custom

USER 1001

