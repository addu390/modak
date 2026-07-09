package io.tierdb.common.mode;

public record UpdatePlan(DeletePlan removeOld, InsertPlan writeNew) {}
