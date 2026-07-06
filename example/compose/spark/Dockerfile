FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY worker/ worker/
COPY connectors/ connectors/
WORKDIR /build/worker
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl ../connectors/modak-spark -am package

FROM eclipse-temurin:17-jre AS jre17

FROM apache/spark:3.5.3
USER root
COPY --from=jre17 /opt/java/openjdk /opt/java17
ENV JAVA_HOME=/opt/java17
ENV PATH="$JAVA_HOME/bin:$PATH"
RUN mkdir -p /opt/modak /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
    && chown -R spark:spark /home/spark/.ivy2
COPY --from=build /build/connectors/modak-spark/target/modak-spark-*.jar /opt/modak/modak-spark.jar
COPY --from=build /build/connectors/modak-connector/target/modak-connector-*.jar /opt/modak/modak-connector.jar
COPY --from=build /build/worker/modak-load/target/modak-load-*.jar /opt/modak/modak-load.jar
COPY --from=build /build/worker/modak-common/target/modak-common-*.jar /opt/modak/modak-common.jar
COPY example/compose/spark/demo.scala /opt/modak/demo.scala
COPY example/compose/spark/entrypoint.sh /opt/modak/entrypoint.sh
RUN chmod +x /opt/modak/entrypoint.sh
USER spark
ENTRYPOINT ["/opt/modak/entrypoint.sh"]
