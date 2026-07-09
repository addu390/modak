package io.tierdb.lake.access;

public interface PredicateRenderer<P> {

    P alwaysTrue();

    P alwaysFalse();

    P isNull(String column);

    P equal(String column, Object value);

    P greaterThanOrEqual(String column, Object value);

    P greaterThan(String column, Object value);

    P lessThanOrEqual(String column, Object value);

    P lessThan(String column, Object value);

    P and(P left, P right);

    P or(P left, P right);
}
