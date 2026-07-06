package io.tierdb.catalog;

import io.tierdb.common.PartitionId;
import io.tierdb.common.PartitionState;

public class IllegalTransitionException extends CatalogException {
    public IllegalTransitionException(PartitionId id, PartitionState from, PartitionState to) {
        super("illegal partition transition for " + id + ": " + from + " -> " + to);
    }
}
