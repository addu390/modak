# Build from the repo root. Trino coordinator with the tierdb plugin and a
# tierdb catalog pointed at the example stack's Postgres and RustFS.

FROM maven:3.9-eclipse-temurin-23 AS build
WORKDIR /build
COPY worker/ worker/
COPY connectors/ connectors/
WORKDIR /build/worker
RUN --mount=type=cache,target=/root/.m2 \
    mvn -q -DskipTests -pl ../connectors/tierdb-trino -am package

FROM trinodb/trino:472
COPY --from=build /build/connectors/tierdb-trino/target/plugin/tierdb /usr/lib/trino/plugin/tierdb
COPY example/compose/trino/tierdb.properties /etc/trino/catalog/tierdb.properties
