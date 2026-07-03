package io.modak.lake;

import io.modak.common.TableId;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Property keys stamped onto every lake snapshot Modak commits, making the
 * commit self-describing. Crash resume filters snapshots by {@link #OP_ID}
 * and recovers the advance from these values.
 */
public final class LakeTieringProps {

    public static final String OP_ID = "modak.op-id";

    // Each op kind's crash-resume probe must only ever claim its own snapshots.
    public static final String OP_KIND = "modak.op-kind";
    public static final String OP_KIND_TIERING = "tiering";
    public static final String OP_KIND_COMPACTION = "compaction";
    public static final String OP_KIND_MIRROR = "mirror";
    public static final String OP_KIND_MAINTENANCE = "maintenance";
    public static final String OP_KIND_RETENTION = "retention";
    public static final String OP_KIND_INGEST = "ingest";

    // The cut-line T this commit supports. Crash resume advances the catalog to it.
    public static final String NEW_TIER_KEY_HI = "modak.new-tier-key-hi";

    // The WAL LSN a mirror commit replicates through (raw long).
    public static final String COMMIT_LSN = "modak.commit-lsn";

    // Identity guard against snapshots from a dropped+re-registered incarnation.
    public static final String TABLE_ID = "modak.table-id";

    // The ecosystem-standard key Iceberg maintenance tools filter on.
    public static final String COMMIT_USER = "commit-user";
    public static final String COMMIT_USER_TIERING = "__modak_tiering";
    public static final String COMMIT_USER_COMPACTION = "__modak_compaction";
    public static final String COMMIT_USER_MIRROR = "__modak_mirror";
    public static final String COMMIT_USER_MAINTENANCE = "__modak_maintenance";
    public static final String COMMIT_USER_RETENTION = "__modak_retention";
    public static final String COMMIT_USER_INGEST = "__modak_ingest";

    /** The standard stamp every op kind puts on its commits, add op-specific keys on top. */
    public static Map<String, String> snapshotProps(UUID opId, String opKind,
            String commitUser, TableId table) {
        Map<String, String> props = new HashMap<>();
        props.put(OP_ID, opId.toString());
        props.put(OP_KIND, opKind);
        props.put(TABLE_ID, Long.toString(table.oid()));
        props.put(COMMIT_USER, commitUser);
        return props;
    }

    private LakeTieringProps() {}
}
