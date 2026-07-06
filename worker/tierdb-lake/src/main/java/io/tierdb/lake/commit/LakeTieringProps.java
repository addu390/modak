package io.tierdb.lake.commit;

import io.tierdb.common.OpKind;
import io.tierdb.common.TableId;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Property keys stamped onto every lake snapshot TierDB commits, making the
 * commit self-describing. Crash resume filters snapshots by {@link #OP_ID}
 * and recovers the advance from these values.
 */
public final class LakeTieringProps {

    public static final String OP_ID = "tierdb.op-id";

    public static final String OP_KIND = "tierdb.op-kind";

    public static final String NEW_TIER_KEY_HI = "tierdb.new-tier-key-hi";

    public static final String COMMIT_LSN = "tierdb.commit-lsn";

    public static final String TABLE_ID = "tierdb.table-id";

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
