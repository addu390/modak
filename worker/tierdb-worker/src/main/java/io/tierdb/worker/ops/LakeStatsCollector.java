package io.tierdb.worker.ops;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.tierdb.lake.LakeStats;
import io.tierdb.worker.Log;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import javax.sql.DataSource;

/** Persists a table's {@link LakeStats} into {@code tierdb.lake_stats} together with the maintenance policy in force. */
public final class LakeStatsCollector {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String UPSERT = """
            INSERT INTO tierdb.lake_stats (table_id, stats, warnings, policy, collected_at)
            VALUES (?, ?::jsonb, ?::jsonb, ?::jsonb, now())
            ON CONFLICT (table_id) DO UPDATE
               SET stats = EXCLUDED.stats,
                   warnings = EXCLUDED.warnings,
                   policy = EXCLUDED.policy,
                   collected_at = now()
            """;

    private final DataSource dataSource;
    private final Map<Long, java.util.List<String>> lastWarnings = new java.util.HashMap<>();

    public LakeStatsCollector(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void record(long tableId, String tableName, LakeStats stats,
            Map<String, String> settings) throws Exception {
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(UPSERT)) {
            ps.setLong(1, tableId);
            ps.setString(2, statsJson(stats));
            ps.setString(3, warningsJson(stats));
            ps.setString(4, settingsJson(settings));
            ps.executeUpdate();
        }
        if (!stats.warnings().equals(lastWarnings.put(tableId, stats.warnings()))) {
            stats.warnings().forEach(w -> Log.warn("%s: lake health: %s", tableName, w));
        }
    }

    private static String statsJson(LakeStats stats) {
        ObjectNode node = MAPPER.createObjectNode();
        stats.values().forEach((key, value) -> {
            if (value == Math.floor(value) && !Double.isInfinite(value)) {
                node.put(key, (long) (double) value);
            } else {
                node.put(key, value);
            }
        });
        return node.toString();
    }

    private static String warningsJson(LakeStats stats) {
        ArrayNode node = MAPPER.createArrayNode();
        stats.warnings().forEach(node::add);
        return node.toString();
    }

    private static String settingsJson(Map<String, String> settings) {
        ObjectNode node = MAPPER.createObjectNode();
        settings.forEach(node::put);
        return node.toString();
    }
}
