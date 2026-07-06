package io.tierdb.worker.cli;

import io.tierdb.catalog.JdbcCatalog;
import io.tierdb.catalog.PartitionInfo;
import io.tierdb.catalog.RegisteredTable;
import io.tierdb.catalog.TableMode;
import io.tierdb.common.PartitionState;
import io.tierdb.common.PgValues;
import io.tierdb.common.RowBatchData.Column;
import io.tierdb.worker.Log;
import io.tierdb.worker.WorkerConfig;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import javax.sql.DataSource;

/** The audit command ({@code tierdb-worker verify}). */
public final class TableVerifier {

    private TableVerifier() {}

    public static int run(WorkerConfig config, String[] args) throws Exception {
        String qualified = new Args(args).required("--table");
        String[] parts = qualified.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("--table must be schema-qualified: " + qualified);
        }
        DataSource ds = config.dataSource();
        JdbcCatalog catalog = new JdbcCatalog(ds);
        RegisteredTable meta = catalog.lookup(parts[0], parts[1])
                .orElseThrow(() -> new IllegalArgumentException(qualified + " is not registered"));

        if (meta.mode() == TableMode.MIRRORED) {
            return verifyMirrored(ds, catalog, meta);
        }
        return meta.keepHeap()
                ? verifyKeepHeap(ds, catalog, meta)
                : verifyTiered(ds, catalog, meta);
    }

    private record Stats(long count, Long minTier, Long maxTier, Long pkHash) {}

    private static int verifyMirrored(DataSource ds, JdbcCatalog catalog, RegisteredTable meta)
            throws Exception {
        String name = meta.schemaName() + "." + meta.tableName();
        String location = metadataLocation(ds, meta);
        List<Column> columns = TableRegistrar.columnsOf(ds, meta.schemaName(), meta.tableName());

        long tierLo = catalog.readCutline(meta.id()).t().value();
        Long bound = tierLo == Long.MIN_VALUE ? null : tierLo;

        Stats heap;
        long walNow;
        try (Connection c = ds.getConnection()) {
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            c.setAutoCommit(false);
            heap = stats(c, heapRowsSql(meta, bound));
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(
                    "SELECT (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint")) {
                rs.next();
                walNow = rs.getLong(1);
            }
            c.commit();
        }

        Stats lake;
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            lake = stats(c, lakeRowsSql(meta, columns, location, bound));
            c.commit();
        }

        Long frontier = null;
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT replicated_lsn FROM tierdb.cutline WHERE table_id = ?")) {
            ps.setLong(1, meta.id().oid());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long f = rs.getLong(1);
                    frontier = rs.wasNull() ? null : f;
                }
            }
        }
        long driftBytes = frontier == null ? -1 : Math.max(0, walNow - frontier);

        boolean pass = heap.equals(lake);
        Log.info("verify %s (mirrored%s):", name, bound == null ? "" : ", tier >= " + bound);
        Log.info("  heap: count=%d tier=[%s..%s] pk_hash=%s",
                heap.count(), heap.minTier(), heap.maxTier(), heap.pkHash());
        Log.info("  lake: count=%d tier=[%s..%s] pk_hash=%s",
                lake.count(), lake.minTier(), lake.maxTier(), lake.pkHash());
        Log.info("  mirror drift: %s", driftBytes < 0 ? "unknown (no frontier)"
                : driftBytes + " WAL byte(s) behind");
        if (pass) {
            Log.info("verify %s: PASS", name);
            return 0;
        }
        Log.error("verify %s: FAIL, heap and lake disagree%s", name,
                driftBytes != 0 ? " (writes may still be in flight, re-run when the "
                        + "mirror drift reaches 0)" : "");
        return 1;
    }

    private static int verifyTiered(DataSource ds, JdbcCatalog catalog, RegisteredTable meta)
            throws Exception {
        String name = meta.schemaName() + "." + meta.tableName();
        String location = metadataLocation(ds, meta);
        List<Column> columns = TableRegistrar.columnsOf(ds, meta.schemaName(), meta.tableName());
        Map<String, Long> journaled = journaledRows(ds, meta);

        boolean pass = true;
        int audited = 0;
        for (PartitionInfo p : catalog.listPartitions(meta.id())) {
            if (p.state() != PartitionState.TIERED && p.state() != PartitionState.DROPPED) {
                continue;
            }
            Long expected = journaled.get(p.id().id());
            if (expected == null) {
                continue;
            }
            long inLake = lakeCountInBounds(ds, meta, columns, location,
                    p.bounds().lo().value(), p.bounds().hi().value());
            audited++;
            if (inLake == 0 && expected > 0) {
                Log.error("verify %s: FAIL, partition %s journaled %d row(s) but the lake "
                        + "holds none in [%d, %d)", name, p.id().id(), expected,
                        p.bounds().lo().value(), p.bounds().hi().value());
                pass = false;
            } else if (inLake != expected) {
                Log.warn("verify %s: partition %s has %d lake row(s) vs %d journaled, "
                        + "delta corrections (late upserts/tombstones) can account for this",
                        name, p.id().id(), inLake, expected);
            } else {
                Log.info("verify %s: partition %s OK (%d row(s))", name, p.id().id(), inLake);
            }
        }
        if (audited == 0) {
            Log.info("verify %s: nothing to audit (no journaled tiered partitions)", name);
        }
        Log.info("verify %s: %s", name, pass ? "PASS" : "FAIL");
        return pass ? 0 : 1;
    }

    private static int verifyKeepHeap(DataSource ds, JdbcCatalog catalog, RegisteredTable meta)
            throws Exception {
        int journalResult = verifyTiered(ds, catalog, meta);

        String name = meta.schemaName() + "." + meta.tableName();
        long tierHi = catalog.readCutline(meta.id()).t().value();
        String location = metadataLocation(ds, meta);
        List<Column> columns = TableRegistrar.columnsOf(ds, meta.schemaName(), meta.tableName());

        Stats heap;
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.execute("SET tierdb.transparent_reads = off");
            }
            heap = stats(c, heapRowsBelowSql(meta, tierHi));
            c.commit();
        }
        Stats lake;
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            lake = stats(c, lakeRowsBelowSql(meta, columns, location, tierHi));
            c.commit();
        }
        long backlog = deltaBacklog(ds, meta);

        boolean pass = heap.equals(lake);
        Log.info("verify %s (keep-heap, tier < %d):", name, tierHi);
        Log.info("  heap: count=%d tier=[%s..%s] pk_hash=%s",
                heap.count(), heap.minTier(), heap.maxTier(), heap.pkHash());
        Log.info("  lake: count=%d tier=[%s..%s] pk_hash=%s",
                lake.count(), lake.minTier(), lake.maxTier(), lake.pkHash());
        Log.info("  delta backlog: %d row(s)", backlog);
        if (pass) {
            Log.info("verify %s: PASS", name);
            return journalResult;
        }
        Log.error("verify %s: FAIL, heap and lake disagree below the cut-line%s", name,
                backlog > 0 ? " (" + backlog + " delta row(s) await compaction, "
                        + "re-run after the fold)" : "");
        return 1;
    }

    private static String heapRowsBelowSql(RegisteredTable meta, long tierHi) {
        String tier = ident(meta.tierKeyCol());
        return "SELECT " + pkExpr(meta.primaryKeyCols()) + ", " + tier + " FROM "
                + ident(meta.schemaName()) + "." + ident(meta.tableName())
                + " WHERE " + tier + " < " + tierHi;
    }

    private static String lakeRowsBelowSql(RegisteredTable meta, List<Column> columns,
            String location, long tierHi) {
        String tier = ident(meta.tierKeyCol());
        List<String> needed = new ArrayList<>(meta.primaryKeyCols());
        if (!needed.contains(meta.tierKeyCol())) {
            needed.add(meta.tierKeyCol());
        }
        return "SELECT " + pkExpr(meta.primaryKeyCols()) + ", " + tier + " FROM ("
                + lakeProjection(needed, columns, location) + ") b"
                + " WHERE " + tier + " < " + tierHi;
    }

    private static long deltaBacklog(DataSource ds, RegisteredTable meta) throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT count(*) FROM tierdb.delta WHERE table_id = ?")) {
            ps.setLong(1, meta.id().oid());
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static String metadataLocation(DataSource ds, RegisteredTable meta) throws Exception {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(
                "SELECT lake_props ->> 'metadata_location' FROM tierdb.cutline WHERE table_id = ?")) {
            ps.setLong(1, meta.id().oid());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next() && rs.getString(1) != null) {
                    return rs.getString(1);
                }
            }
        }
        throw new IllegalStateException("no metadata_location published for "
                + meta.schemaName() + "." + meta.tableName());
    }

    private static Stats stats(Connection c, String sql) throws Exception {
        try (Statement s = c.createStatement()) {
            s.setFetchSize(50_000);
            try (ResultSet rs = s.executeQuery(sql)) {
                long count = 0;
                Long minTier = null;
                Long maxTier = null;
                long sum = 0;
                while (rs.next()) {
                    count++;
                    CRC32 crc = new CRC32();
                    crc.update(rs.getString(1).getBytes(StandardCharsets.UTF_8));
                    sum += crc.getValue();
                    long tier = rs.getLong(2);
                    minTier = minTier == null ? tier : Math.min(minTier, tier);
                    maxTier = maxTier == null ? tier : Math.max(maxTier, tier);
                }
                return new Stats(count, minTier, maxTier, count == 0 ? null : sum);
            }
        }
    }

    private static String heapRowsSql(RegisteredTable meta, Long tierLo) {
        String tier = ident(meta.tierKeyCol());
        return "SELECT " + pkExpr(meta.primaryKeyCols()) + ", " + tier + " FROM "
                + ident(meta.schemaName()) + "." + ident(meta.tableName())
                + (tierLo == null ? "" : " WHERE " + tier + " >= " + tierLo);
    }

    private static String lakeRowsSql(RegisteredTable meta, List<Column> columns,
            String location, Long tierLo) {
        String tier = ident(meta.tierKeyCol());
        List<String> needed = new ArrayList<>(meta.primaryKeyCols());
        if (!needed.contains(meta.tierKeyCol())) {
            needed.add(meta.tierKeyCol());
        }
        return "SELECT " + pkExpr(meta.primaryKeyCols()) + ", " + tier + " FROM ("
                + lakeProjection(needed, columns, location) + ") b"
                + (tierLo == null ? "" : " WHERE " + tier + " >= " + tierLo);
    }

    private static long lakeCountInBounds(DataSource ds, RegisteredTable meta,
            List<Column> columns, String location, long lo, long hi) throws Exception {
        String tier = ident(meta.tierKeyCol());
        List<String> needed = new ArrayList<>(meta.primaryKeyCols());
        if (!needed.contains(meta.tierKeyCol())) {
            needed.add(meta.tierKeyCol());
        }
        String sql = "SELECT count(" + pkExpr(meta.primaryKeyCols()) + ") FROM ("
                + lakeProjection(needed, columns, location) + ") b"
                + " WHERE " + tier + " >= " + lo + " AND " + tier + " < " + hi;
        try (Connection c = ds.getConnection(); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static String lakeProjection(List<String> names, List<Column> columns,
            String location) {
        StringBuilder inner = new StringBuilder("SELECT ");
        StringBuilder outer = new StringBuilder("SELECT ");
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) {
                inner.append(", ");
                outer.append(", ");
            }
            String name = names.get(i);
            Column col = columns.stream().filter(c -> c.name().equals(name)).findFirst()
                    .orElseThrow(() -> new IllegalStateException("column '" + name
                            + "' missing from the heap relation"));
            String cast = PgValues.castSuffix(col.type());
            inner.append(ident(name));
            outer.append("r[").append(lit(name)).append(']')
                    .append(cast.isEmpty() ? "::text" : cast)
                    .append(" AS ").append(ident(name));
        }
        inner.append(" FROM iceberg_scan(").append(lit(location)).append(')');
        return outer + " FROM duckdb.query(" + lit(inner.toString()) + ") r";
    }

    private static String pkExpr(List<String> pkCols) {
        if (pkCols.size() == 1) {
            return ident(pkCols.get(0)) + "::text";
        }
        StringBuilder sb = new StringBuilder("concat_ws(chr(31)");
        for (String pk : pkCols) {
            sb.append(", ").append(ident(pk)).append("::text");
        }
        return sb.append(')').toString();
    }

    private static Map<String, Long> journaledRows(DataSource ds, RegisteredTable meta)
            throws Exception {
        Map<String, Long> out = new HashMap<>();
        String sql = """
                SELECT kv.key, sum(kv.value::bigint)
                  FROM tierdb.op_log t,
                       jsonb_each_text(t.details -> 'partition_rows') kv
                 WHERE t.table_id = ? AND t.op_kind = 'tiering' AND t.phase = 'advanced'
                 GROUP BY kv.key
                """;
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, meta.id().oid());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), rs.getLong(2));
                }
            }
        }
        return out;
    }

    private static String ident(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    private static String lit(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
