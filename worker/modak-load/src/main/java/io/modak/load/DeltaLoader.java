package io.modak.load;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

public final class DeltaLoader {

    public static final String UPSERT_SQL = """
            INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload)
            VALUES (?, ?, ?, ?, nextval('modak.delta_version'), ?::jsonb)
            ON CONFLICT (table_id, pk) DO UPDATE
               SET op = excluded.op, tier_key = excluded.tier_key,
                   old_tier_key = nullif(
                       coalesce(modak.delta.old_tier_key, modak.delta.tier_key),
                       excluded.tier_key),
                   version = excluded.version,
                   payload = excluded.payload, updated_at = now()
             WHERE modak.delta.version < excluded.version
            """;

    public static final int BATCH_SIZE = 500;

    public static final int OP_UPSERT = 0;
    public static final int OP_TOMBSTONE = 1;

    public record Entry(String pk, int op, long tierKey, String payloadJson) {

        public static Entry upsert(String pk, long tierKey, String payloadJson) {
            return new Entry(pk, OP_UPSERT, tierKey, payloadJson);
        }

        public static Entry tombstone(String pk, long tierKey, String payloadJson) {
            return new Entry(pk, OP_TOMBSTONE, tierKey, payloadJson);
        }
    }

    private DeltaLoader() {}

    public static long upsert(Connection c, long tableId, Iterator<Entry> entries)
            throws SQLException {
        long total = 0;
        try (PreparedStatement ps = c.prepareStatement(UPSERT_SQL)) {
            int pending = 0;
            while (entries.hasNext()) {
                Entry e = entries.next();
                ps.setLong(1, tableId);
                ps.setString(2, e.pk());
                ps.setInt(3, e.op());
                ps.setLong(4, e.tierKey());
                ps.setString(5, e.payloadJson());
                ps.addBatch();
                total++;
                if (++pending == BATCH_SIZE) {
                    ps.executeBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                ps.executeBatch();
            }
        }
        return total;
    }
}
