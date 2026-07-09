package io.tierdb.common.mode;

import java.util.Optional;

public record InsertPlan(boolean toHeap, Optional<ColdSink> cold, boolean checkRetention) {}
