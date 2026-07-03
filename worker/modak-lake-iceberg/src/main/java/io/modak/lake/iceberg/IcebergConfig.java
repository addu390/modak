package io.modak.lake.iceberg;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * Builds the Hadoop {@link Configuration} for a warehouse from the opaque lake
 * config: a local path needs no keys, S3-compatible stores are selected purely by
 * {@code s3.*} keys, and {@code hadoop.*} keys pass through verbatim.
 */
public final class IcebergConfig {

    private IcebergConfig() {}

    public static Configuration hadoopConf(Map<String, String> config) {
        Configuration conf = new Configuration();
        for (Map.Entry<String, String> e : config.entrySet()) {
            if (e.getKey().startsWith("hadoop.")) {
                conf.set(e.getKey().substring("hadoop.".length()), e.getValue());
            }
        }
        if (config.containsKey("s3.endpoint") || config.containsKey("s3.access-key")) {
            // Table locations use s3:// (not s3a://) so the paths recorded in Iceberg
            // manifests are directly readable by DuckDB's httpfs. Hadoop still routes
            // them through the s3a filesystem via this mapping.
            conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            setIfPresent(conf, config, "s3.endpoint", "fs.s3a.endpoint");
            setIfPresent(conf, config, "s3.access-key", "fs.s3a.access.key");
            setIfPresent(conf, config, "s3.secret-key", "fs.s3a.secret.key");
            // AWS SDK v2 refuses to build a client without *some* region, MinIO ignores it.
            conf.set("fs.s3a.endpoint.region", config.getOrDefault("s3.region", "us-east-1"));
            conf.set("fs.s3a.path.style.access",
                    config.getOrDefault("s3.path-style-access", "true"));
            conf.set("fs.s3a.connection.ssl.enabled",
                    config.getOrDefault("s3.ssl-enabled", "false"));
        }
        return conf;
    }

    private static void setIfPresent(Configuration conf, Map<String, String> config,
            String key, String hadoopKey) {
        String v = config.get(key);
        if (v != null) {
            conf.set(hadoopKey, v);
        }
    }
}
