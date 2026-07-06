package io.modak.load;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modak.catalog.LoadState;
import io.modak.connector.seam.SeamClient;
import io.modak.connector.seam.SeamState;
import io.modak.lake.LakeStorage;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * The Stream Load entry point: one call applies one labeled micro-batch
 * exactly once, everything committing in one Postgres transaction.
 */
public final class LoadClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final LoadOptions options;
    private final Function<String, LakeStorage> lakeByFormat;

    public LoadClient(LoadOptions options) {
        this(options, format -> null);
    }

    public LoadClient(LoadOptions options, Function<String, LakeStorage> lakeByFormat) {
        this.options = Objects.requireNonNull(options);
        this.lakeByFormat = Objects.requireNonNull(lakeByFormat);
    }

    public LoadResult load(LoadRequest request) {
        SeamState state = SeamClient.capture(options.seam(), false);
        LakeStorage lake = state.heapIsComplete() ? null
                : lakeByFormat.apply(state.lakeFormat());

        BatchRouter.Routed routed = BatchRouter.route(request.rows(), state,
                lake != null, options.spoolThreshold());
        List<String> columns = columnsOf(request.rows(), state.tierKeyCol());

        StagedFiles spooled = routed.spools()
                ? Spooler.spool(lake, state, columns, routed.coldSpool())
                : null;

        LoadResult result = new LoadResult(request.label(),
                spooled != null ? LoadState.STAGED : LoadState.COMMITTED,
                routed.hot().size(), routed.coldDelta().size(), routed.coldSpool().size(),
                spooled != null ? spooled.files() : List.of(), false);

        try (Connection c = SeamClient.connect(options.seam())) {
            try (var s = c.createStatement()) {
                s.execute("SET modak.transparent_reads = off");
                s.execute("SET modak.transparent_writes = off");
            }
            c.setAutoCommit(false);
            try {
                if (!LabelRegistry.tryLock(c, state.tableId(), request.label())) {
                    throw new LoadInFlightException("a load labeled '" + request.label()
                            + "' is in flight for " + options.seam().qualifiedName());
                }
                boolean fresh = LabelRegistry.insert(c, state.tableId(), request.label(),
                        result.state(), spooled != null ? spooled.toJson() : null,
                        result.toJson());
                if (!fresh) {
                    c.rollback();
                    return replay(c, state.tableId(), request.label());
                }
                HeapLoader.upsert(c, options.seam().schemaName(), options.seam().tableName(),
                        state.primaryKeyCols(), columns, routed.hot());
                DeltaLoader.upsert(c, state.tableId(),
                        deltaEntries(routed.coldDelta(), state).iterator());
                c.commit();
            } catch (SQLException | RuntimeException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new LoadException("load '" + request.label() + "' into "
                    + options.seam().qualifiedName() + " failed: " + e.getMessage(), e);
        }
        return result;
    }

    private LoadResult replay(Connection c, long tableId, String label) throws SQLException {
        LabelRegistry.Stored stored = LabelRegistry.lookup(c, tableId, label)
                .orElseThrow(() -> new LoadException(
                        "label '" + label + "' conflicted but has no stored row"));
        if (stored.resultJson() == null) {
            return new LoadResult(label, stored.state(), 0, 0, 0, List.of(), true);
        }
        LoadResult recorded = LoadResult.fromJson(stored.resultJson(), true);
        return new LoadResult(recorded.label(), stored.state(), recorded.hotRows(),
                recorded.deltaRows(), recorded.spooledRows(), recorded.stagedFiles(), true);
    }

    private static List<DeltaLoader.Entry> deltaEntries(List<Map<String, Object>> rows,
            SeamState state) {
        List<DeltaLoader.Entry> entries = new ArrayList<>(rows.size());
        int lineNo = 0;
        for (Map<String, Object> row : rows) {
            lineNo++;
            try {
                entries.add(DeltaLoader.Entry.upsert(
                        BatchRouter.encodePk(row, state.primaryKeyCols(), lineNo),
                        BatchRouter.tierKey(row, state, lineNo),
                        MAPPER.writeValueAsString(row)));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                throw new LoadException("row is not JSON-encodable", e);
            }
        }
        return entries;
    }

    private static List<String> columnsOf(List<Map<String, Object>> rows, String tierKeyCol) {
        Set<String> columns = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) {
            columns.addAll(row.keySet());
        }
        if (!rows.isEmpty() && !columns.contains(tierKeyCol)) {
            throw new LoadException("the batch never mentions tier-key column '"
                    + tierKeyCol + "'");
        }
        return List.copyOf(columns);
    }
}
