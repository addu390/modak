package io.tierdb.worker.ops;

import io.tierdb.catalog.RegisteredTable;
import io.tierdb.lake.LakeTable;
import io.tierdb.load.StagedFiles;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

/** Journal-driven cleanup: deletes staged files left behind by failed Stream Loads that no commit will ever adopt. */
public final class StagedFileJanitor {

    private static final String FAILED_WITH_FILES = """
            SELECT label, staged_files FROM tierdb.load_labels
             WHERE table_id = ? AND state = 'failed' AND staged_files IS NOT NULL
               AND updated_at < now() - make_interval(hours => ?)
            """;

    private static final String CLEAR_MANIFEST = """
            UPDATE tierdb.load_labels SET staged_files = NULL, updated_at = now()
             WHERE table_id = ? AND label = ? AND state = 'failed'
            """;

    private final DataSource dataSource;

    public StagedFileJanitor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public int run(RegisteredTable table, LakeTable lakeTable, Map<String, String> settings)
            throws Exception {
        int graceHours = Integer.parseInt(settings.getOrDefault("orphan_grace_hours", "72"));
        record Doomed(String label, List<String> files) {}
        List<Doomed> doomed = new ArrayList<>();
        try (Connection c = dataSource.getConnection();
                PreparedStatement ps = c.prepareStatement(FAILED_WITH_FILES)) {
            ps.setLong(1, table.id().oid());
            ps.setInt(2, graceHours);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String label = rs.getString(1);
                    StagedFiles.fromJson(rs.getString(2))
                            .ifPresent(staged -> doomed.add(new Doomed(label, staged.files())));
                }
            }
        }
        int deleted = 0;
        for (Doomed d : doomed) {
            lakeTable.deleteFiles(d.files());
            deleted += d.files().size();
            try (Connection c = dataSource.getConnection();
                    PreparedStatement ps = c.prepareStatement(CLEAR_MANIFEST)) {
                ps.setLong(1, table.id().oid());
                ps.setString(2, d.label());
                ps.executeUpdate();
            }
        }
        return deleted;
    }
}
