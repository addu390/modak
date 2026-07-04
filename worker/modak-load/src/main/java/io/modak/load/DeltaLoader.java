package io.modak.load;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * The one copy of the {@code modak.delta} upsert every connector shares,
 * newest-wins via {@code modak.delta_version}.
 */
public final class DeltaLoader {

    public static final String UPSERT_SQL = """
            INSERT INTO modak.delta (table_id, pk, op, tier_key, version, payload)
            VALUES (?, ?, 0, ?, nextval('modak.delta_version'), ?::jsonb)
            ON CONFLICT (table_id, pk) DO UPDATE
               SET op = 0, tier_key = excluded.tier_key,
                   old_tier_key = nullif(
                       coalesce(modak.delta.old_tier_key, modak.delta.tier_key),
                       excluded.tier_key),
                   version = excluded.version,
                   payload = excluded.payload, updated_at = now()
             WHERE modak.delta.version < excluded.version
            """;

    public static final int BATCH_SIZE = 500;

    public record Entry(String pk, long tierKey, String payloadJson) {}

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
                ps.setLong(3, e.tierKey());
                ps.setString(4, e.payloadJson());
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
