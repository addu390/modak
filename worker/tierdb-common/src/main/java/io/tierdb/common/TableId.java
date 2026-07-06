package io.tierdb.common;

/** Identity of a TierDB-managed logical table (a Postgres relation OID). */
public record TableId(long oid) {}
