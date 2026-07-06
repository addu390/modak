# Build from the repo root. Trino coordinator with the modak plugin and a
# modak catalog pointed at the example stack's Postgres and RustFS.

FROM maven:3.9-eclipse-temurin-23 AS build
WORKDIR /build
COPY worker/ worker/
COPY connectors/ connectors/
WORKDIR /build/worker
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl ../connectors/modak-trino -am package

FROM trinodb/trino:472
COPY --from=build /build/connectors/modak-trino/target/plugin/modak /usr/lib/trino/plugin/modak
COPY example/compose/trino/modak.properties /etc/trino/catalog/modak.properties
