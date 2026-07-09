package io.tierdb.lake.iceberg.access;

import io.tierdb.common.RowBatchData.Column;
import io.tierdb.lake.access.ColumnConstraint;
import io.tierdb.lake.access.ConstraintRenderer;
import io.tierdb.lake.access.PredicateRenderer;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

public final class IcebergPredicates implements PredicateRenderer<Expression> {

    public static final IcebergPredicates INSTANCE = new IcebergPredicates();

    private IcebergPredicates() {}

    public static Optional<Expression> expression(Map<Column, ColumnConstraint> constraints) {
        return ConstraintRenderer.render(constraints, INSTANCE);
    }

    @Override
    public Expression alwaysTrue() {
        return Expressions.alwaysTrue();
    }

    @Override
    public Expression alwaysFalse() {
        return Expressions.alwaysFalse();
    }

    @Override
    public Expression isNull(String column) {
        return Expressions.isNull(column);
    }

    @Override
    public Expression equal(String column, Object value) {
        return Expressions.equal(column, literal(value));
    }

    @Override
    public Expression greaterThanOrEqual(String column, Object value) {
        return Expressions.greaterThanOrEqual(column, literal(value));
    }

    @Override
    public Expression greaterThan(String column, Object value) {
        return Expressions.greaterThan(column, literal(value));
    }

    @Override
    public Expression lessThanOrEqual(String column, Object value) {
        return Expressions.lessThanOrEqual(column, literal(value));
    }

    @Override
    public Expression lessThan(String column, Object value) {
        return Expressions.lessThan(column, literal(value));
    }

    @Override
    public Expression and(Expression left, Expression right) {
        return Expressions.and(left, right);
    }

    @Override
    public Expression or(Expression left, Expression right) {
        return Expressions.or(left, right);
    }

    private static Object literal(Object value) {
        return value instanceof LocalDate date ? (int) date.toEpochDay() : value;
    }
}
