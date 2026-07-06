#!/bin/sh
set -e

exec /opt/spark/bin/spark-shell \
    --jars /opt/tierdb/tierdb-spark.jar,/opt/tierdb/tierdb-connector.jar,/opt/tierdb/tierdb-load.jar,/opt/tierdb/tierdb-common.jar \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.spark_catalog.type=rest \
    --conf spark.sql.catalog.spark_catalog.uri="$TIERDB_CATALOG_URI" \
    --conf spark.sql.catalog.spark_catalog.warehouse="$TIERDB_CATALOG_WAREHOUSE" \
    --conf spark.sql.catalog.spark_catalog.io-impl=org.apache.iceberg.hadoop.HadoopFileIO \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.endpoint="$TIERDB_S3_ENDPOINT" \
    --conf spark.hadoop.fs.s3a.access.key="$TIERDB_S3_ACCESS_KEY" \
    --conf spark.hadoop.fs.s3a.secret.key="$TIERDB_S3_SECRET_KEY" \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    -i /opt/tierdb/demo.scala
