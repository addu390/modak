package io.tierdb.lake.access;

import java.io.Serializable;
import java.util.Map;

/**
 * A resolved lake scan as opaque per-format properties (the same vocabulary a
 * format publishes in {@code tierdb.cutline.lake_props}). Pinned and live
 * resolution produce the same keys, so readers never distinguish the two.
 */
public record LakeScan(Map<String, String> props) implements Serializable {}
