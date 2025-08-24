# --- build stage ---
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src src
RUN mvn -B -DskipTests package

# --- run stage ---
FROM flink:1.18.1-scala_2.12-java17
WORKDIR /job
COPY --from=builder /build/target/flink-tx-nowindow-1.0.0.jar /job/app.jar

ENV brokers=kafka:29092 \
    in=transactions \
    out=alerts.duplicate_amount \
    group=flink-tx-nowindow-1

ENTRYPOINT ["/bin/bash","-lc"]
CMD ["/opt/flink/bin/flink run -m flink-jobmanager:8081 \
 -Dbrokers=${brokers} -Din=${in} -Dout=${out} -Dgroup=${group} \
 -c com.example.TxDublicationCheck /job/app.jar"]
