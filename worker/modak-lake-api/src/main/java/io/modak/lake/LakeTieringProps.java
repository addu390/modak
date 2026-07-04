package io.modak.lake;

import io.modak.common.OpKind;
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

    // The cut-line T this commit supports. Crash resume advances the catalog to it.
    public static final String NEW_TIER_KEY_HI = "modak.new-tier-key-hi";

    // The WAL LSN a mirror commit replicates through (raw long).
    public static final String COMMIT_LSN = "modak.commit-lsn";

    // Identity guard against snapshots from a dropped+re-registered incarnation.
    public static final String TABLE_ID = "modak.table-id";

    // The ecosystem-standard key Iceberg maintenance tools filter on.
    public static final String COMMIT_USER = "commit-user";

    /** The standard stamp every op kind puts on its commits, add op-specific keys on top. */
    public static Map<String, String> snapshotProps(UUID opId, OpKind kind, TableId table) {
        Map<String, String> props = new HashMap<>();
        props.put(OP_ID, opId.toString());
        props.put(OP_KIND, kind.sql());
        props.put(TABLE_ID, Long.toString(table.oid()));
        props.put(COMMIT_USER, kind.commitUser());
        return props;
    }

    private LakeTieringProps() {}
}
