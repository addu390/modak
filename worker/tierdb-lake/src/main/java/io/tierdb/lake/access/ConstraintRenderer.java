package io.tierdb.lake.access;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.common.RowBatchData.ColumnType;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class ConstraintRenderer {

    private static final Set<ColumnType> PUSHABLE = EnumSet.of(
            ColumnType.BOOLEAN, ColumnType.LONG, ColumnType.DOUBLE,
            ColumnType.DATE, ColumnType.TEXT);

    private ConstraintRenderer() {}

    public static <P> Optional<P> render(Map<Column, ColumnConstraint> constraints,
            PredicateRenderer<P> renderer) {
        P combined = null;
        for (Map.Entry<Column, ColumnConstraint> entry : constraints.entrySet()) {
            if (!PUSHABLE.contains(entry.getKey().type())) {
                continue;
            }
            Optional<P> predicate = renderColumn(entry.getKey(), entry.getValue(), renderer);
            if (predicate.isPresent()) {
                combined = combined == null ? predicate.get() : renderer.and(combined, predicate.get());
            }
        }
        return Optional.ofNullable(combined);
    }

    private static <P> Optional<P> renderColumn(Column column, ColumnConstraint constraint,
            PredicateRenderer<P> renderer) {
        if (constraint instanceof ColumnConstraint.None) {
            return Optional.of(renderer.alwaysFalse());
        }
        if (constraint instanceof ColumnConstraint.OnlyNull) {
            return Optional.of(renderer.isNull(column.name()));
        }
        return renderRanges(column.name(), (ColumnConstraint.Ranges) constraint, renderer);
    }

    private static <P> Optional<P> renderRanges(String name, ColumnConstraint.Ranges ranges,
            PredicateRenderer<P> renderer) {
        List<P> disjuncts = new ArrayList<>();
        for (ColumnConstraint.ValueRange range : ranges.ranges()) {
            disjuncts.add(renderRange(name, range, renderer));
        }
        if (ranges.nullAllowed()) {
            disjuncts.add(renderer.isNull(name));
        }
        if (disjuncts.isEmpty()) {
            return Optional.empty();
        }
        P union = disjuncts.get(0);
        for (int i = 1; i < disjuncts.size(); i++) {
            union = renderer.or(union, disjuncts.get(i));
        }
        return Optional.of(union);
    }

    private static <P> P renderRange(String name, ColumnConstraint.ValueRange range,
            PredicateRenderer<P> renderer) {
        if (range.isSingleValue()) {
            return renderer.equal(name, range.low());
        }
        P expr = null;
        if (range.low() != null) {
            expr = range.lowInclusive() ? renderer.greaterThanOrEqual(name, range.low())
                    : renderer.greaterThan(name, range.low());
        }
        if (range.high() != null) {
            P highExpr = range.highInclusive() ? renderer.lessThanOrEqual(name, range.high())
                    : renderer.lessThan(name, range.high());
            expr = expr == null ? highExpr : renderer.and(expr, highExpr);
        }
        return expr == null ? renderer.alwaysTrue() : expr;
    }
}
