FROM maven:3.6-jdk-11-slim AS builder

COPY ./flink-cdc /opt/flink-cdc
WORKDIR /opt/flink-cdc
RUN mvn clean package -DskipTests

FROM flink:1.11.2-scala_2.12-java11

WORKDIR /opt/flink/bin

# kafkacat & sample data for demo
RUN apt-get update && apt-get install -yq kafkacat
COPY *.csv /opt/

# Copy job jar
COPY --from=builder /opt/flink-cdc/target/flink-cdc-*.jar /opt/flink-job.jar
