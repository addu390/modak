package io.tierdb.common.mode;

import java.util.Optional;

public record DeletePlan(
        boolean fromHeap, boolean heapFloor, Optional<ColdSink> cold, boolean checkRetention) {}
