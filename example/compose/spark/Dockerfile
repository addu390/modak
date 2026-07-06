FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY worker/ worker/
COPY connectors/ connectors/
WORKDIR /build/worker
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl ../connectors/tierdb-spark -am package

FROM eclipse-temurin:17-jre AS jre17

FROM apache/spark:3.5.3
USER root
COPY --from=jre17 /opt/java/openjdk /opt/java17
ENV JAVA_HOME=/opt/java17
ENV PATH="$JAVA_HOME/bin:$PATH"
RUN mkdir -p /opt/tierdb /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2
COPY --from=build /build/connectors/tierdb-spark/target/tierdb-spark-*.jar /opt/tierdb/tierdb-spark.jar
COPY --from=build /build/connectors/tierdb-connector/target/tierdb-connector-*.jar /opt/tierdb/tierdb-connector.jar
COPY --from=build /build/worker/tierdb-load/target/tierdb-load-*.jar /opt/tierdb/tierdb-load.jar
COPY --from=build /build/worker/tierdb-common/target/tierdb-common-*.jar /opt/tierdb/tierdb-common.jar
COPY example/compose/spark/demo.scala /opt/tierdb/demo.scala
COPY example/compose/spark/entrypoint.sh /opt/tierdb/entrypoint.sh
RUN chmod +x /opt/tierdb/entrypoint.sh
USER spark
ENTRYPOINT ["/opt/tierdb/entrypoint.sh"]
