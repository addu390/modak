package io.modak.lake.commit;

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

    public static final String OP_KIND = "modak.op-kind";

    public static final String NEW_TIER_KEY_HI = "modak.new-tier-key-hi";

    public static final String COMMIT_LSN = "modak.commit-lsn";

    public static final String TABLE_ID = "modak.table-id";

    public static final String COMMIT_USER = "commit-user";

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
