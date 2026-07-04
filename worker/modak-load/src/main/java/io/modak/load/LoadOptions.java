package io.modak.load;

import io.modak.connector.SeamOptions;
import java.util.Objects;

/**
 * How one {@link LoadClient} reaches its table: connection rides on
 * {@link SeamOptions}, {@code spoolThreshold} decides when cold rows spool.
 */
public final class LoadOptions {

    public static final int DEFAULT_SPOOL_THRESHOLD = 1000;

    private final SeamOptions seam;
    private final int spoolThreshold;

    private LoadOptions(Builder b) {
        SeamOptions.Builder seam = SeamOptions.builder()
                .jdbcUrl(Objects.requireNonNull(b.jdbcUrl, "jdbcUrl"))
                .table(Objects.requireNonNull(b.table, "table"));
        if (b.user != null) {
            seam.jdbcProperty("user", b.user);
        }
        if (b.password != null) {
            seam.jdbcProperty("password", b.password);
        }
        this.seam = seam.build();
        this.spoolThreshold = b.spoolThreshold;
    }

    public static Builder builder() {
        return new Builder();
    }

    public SeamOptions seam() {
        return seam;
    }

    public int spoolThreshold() {
        return spoolThreshold;
    }

    public static final class Builder {

        private String jdbcUrl;
        private String user;
        private String password;
        private String table;
        private int spoolThreshold = DEFAULT_SPOOL_THRESHOLD;

        public Builder jdbcUrl(String url) {
            this.jdbcUrl = url;
            return this;
        }

        public Builder credentials(String user, String password) {
            this.user = user;
            this.password = password;
            return this;
        }

        public Builder table(String qualifiedName) {
            this.table = qualifiedName;
            return this;
        }

        /** Cold rows above this count per batch are spooled as parquet (default 1000). */
        public Builder spoolThreshold(int threshold) {
            this.spoolThreshold = threshold;
            return this;
        }

        public LoadOptions build() {
            return new LoadOptions(this);
        }
    }
}
