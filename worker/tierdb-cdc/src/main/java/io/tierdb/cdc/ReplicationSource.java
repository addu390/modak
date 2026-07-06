package io.tierdb.cdc;

import io.tierdb.common.Lsn;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

/**
 * Slot + publication lifecycle and a thin wrapper over PgJDBC's {@link
 * PGReplicationStream} ({@code pgoutput} is built into core Postgres, so
 * managed Postgres works).
 */
public final class ReplicationSource implements AutoCloseable {

    private final Connection connection;
    private final PGReplicationStream stream;

    private ReplicationSource(Connection connection, PGReplicationStream stream) {
        this.connection = connection;
        this.stream = stream;
    }

    public static ReplicationSource open(String url, String user, String password,
            String slot, String publication, Lsn start) {
        terminateStaleHolder(url, user, password, slot);
        try {
            Connection conn = replicationConnection(url, user, password);
            var builder = conn.unwrap(PGConnection.class).getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(slot)
                    .withSlotOption("proto_version", "1")
                    .withSlotOption("publication_names", publication)
                    .withStatusInterval(10, TimeUnit.SECONDS);
            if (start.value() != 0) {
                builder = builder.withStartPosition(LogSequenceNumber.valueOf(start.value()));
            }
            return new ReplicationSource(conn, builder.start());
        } catch (SQLException e) {
            throw new CdcException("failed to open replication stream on slot " + slot, e);
        }
    }

    private static void terminateStaleHolder(String url, String user, String password,
            String slot) {
        Properties props = new Properties();
        PGProperty.USER.set(props, user);
        PGProperty.PASSWORD.set(props, password);
        try (Connection c = DriverManager.getConnection(url, props);
                var ps = c.prepareStatement(
                        "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots "
                                + "WHERE slot_name = ? AND active_pid IS NOT NULL")) {
            ps.setString(1, slot);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Thread.sleep(200);
                }
            }
        } catch (SQLException ignored) {
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public PgOutputMessage poll() {
        try {
            ByteBuffer raw = stream.readPending();
            return raw == null ? null : PgOutputDecoder.decode(raw);
        } catch (SQLException e) {
            throw new CdcException("replication stream read failed", e);
        }
    }

    public Lsn lastReceived() {
        return new Lsn(stream.getLastReceiveLSN().asLong());
    }

    public void reportFlushed(Lsn lsn) {
        LogSequenceNumber pos = LogSequenceNumber.valueOf(lsn.value());
        stream.setFlushedLSN(pos);
        stream.setAppliedLSN(pos);
        try {
            stream.forceUpdateStatus();
        } catch (SQLException e) {
            throw new CdcException("failed to send slot feedback for " + lsn, e);
        }
    }

    @Override
    public void close() {
        try (connection) {
            stream.close();
        } catch (SQLException e) {
            throw new CdcException("failed to close replication stream", e);
        }
    }

    /** The slot's birth certificate: where streaming starts and the snapshot the initial copy must use. */
    public record SlotCreation(String slotName, Lsn consistentPoint, String snapshotName) {}

    public static SlotCreation createSlotWithExportedSnapshot(Connection replConn, String slot) {
        String sql = "CREATE_REPLICATION_SLOT \"" + slot.replace("\"", "\"\"")
                + "\" LOGICAL pgoutput EXPORT_SNAPSHOT";
        try (Statement s = replConn.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) {
                throw new CdcException("CREATE_REPLICATION_SLOT returned no row for " + slot);
            }
            return new SlotCreation(
                    rs.getString("slot_name"),
                    Lsn.fromPg(rs.getString("consistent_point")),
                    rs.getString("snapshot_name"));
        } catch (SQLException e) {
            throw new CdcException("failed to create replication slot " + slot, e);
        }
    }

    public static Connection replicationConnection(String url, String user, String password) {
        Properties props = new Properties();
        PGProperty.USER.set(props, user);
        PGProperty.PASSWORD.set(props, password);
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        try {
            return DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            throw new CdcException("failed to open a replication connection to " + url, e);
        }
    }

    public static void createPublication(Connection c, String publication, String qualifiedTable) {
        exec(c, "CREATE PUBLICATION " + ident(publication) + " FOR TABLE " + qualifiedTable
                + " WITH (publish_via_partition_root = true)");
    }

    public static void dropPublication(Connection c, String publication) {
        exec(c, "DROP PUBLICATION IF EXISTS " + ident(publication));
    }

    public static void dropSlot(Connection c, String slot) {
        try (var ps = c.prepareStatement(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                        + "WHERE slot_name = ?")) {
            ps.setString(1, slot);
            ps.execute();
        } catch (SQLException e) {
            throw new CdcException("failed to drop replication slot " + slot, e);
        }
    }

    private static void exec(Connection c, String sql) {
        try (Statement s = c.createStatement()) {
            s.execute(sql);
        } catch (SQLException e) {
            throw new CdcException("failed: " + sql, e);
        }
    }

    private static String ident(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
