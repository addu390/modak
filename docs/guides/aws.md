# Deploying on AWS

Modak on AWS is three decisions: which Postgres holds the hot tier, where the
worker runs, and how the lake is cataloged. The first one decides how much of
Modak you get.

## Choosing the hot tier

- Self-managed Postgres (EC2, or EKS with an operator): everything works.
  Install the extension, preload it, run the worker.
- RDS and Aurora: managed Postgres forbids custom native extensions, so the
  `modak` extension cannot be installed. There are no transparent reads and no
  write routing inside Postgres. The worker runs fine against both, and
  [connectors](../connectors/index.md) take over the read side.

## RDS and Aurora

The worker needs nothing exotic, just JDBC and logical replication:

- Set `rds.logical_replication = 1` in the parameter group (it sets
  `wal_level = logical`) and reboot.
- Grant the worker role `rds_replication` so it can create and stream
  replication slots.
- Cap slot growth with `max_slot_wal_keep_size` in the parameter group. The
  reasoning is the same as in
  [Production deployment](production.md#wal-safety): a dead consumer must
  never fill the disk, and on RDS a full disk is a support ticket.

Mirrored tables are the natural fit for managed Postgres. The heap stays
complete, the application reads and writes plain Postgres with no extension
involved, and CDC keeps the lake copy fresh for analytics.

Tiered tables shed heap data, and without the extension Postgres alone cannot
serve reads below the cut-line. Register tiered tables on RDS only when every
consumer of cold data goes through a connector such as
[Spark](../connectors/spark.md).

## The lake

- Warehouse: an S3 bucket, `MODAK_WAREHOUSE=s3://bucket/modak`. Leave the
  access keys unset and credentials come from the task role (ECS) or IRSA
  (EKS) through the default chain.
- Catalog: path-based is the default and needs no service at all. For a REST
  catalog, run Lakekeeper or Polaris next to the worker.
- Glue and S3 Tables expose Iceberg REST endpoints, but both sign requests
  with SigV4, which the worker's REST client does not speak yet. Use
  path-based or a standard REST catalog for now. What Modak writes is
  standard Iceberg on S3 either way, so Athena or Glue jobs can still be
  pointed at the warehouse.

## Running the worker

The worker is stateless, so it deploys like any service: an ECS service or an
EKS Deployment. Run two replicas. They campaign for a leader lease and fail
over automatically, including the replication slots of mirrored tables. No
persistent volumes, and Prometheus scrapes `/metrics`.

One rollout gotcha: the worker exits on SIGTERM without draining in-flight
lake commits. This is safe, replay heals on restart, but it adds catch-up
time right after a deploy.

## Self-managed Postgres on EKS

Running Postgres yourself (CloudNativePG or another operator) restores the
full experience:

- Build the Postgres image with the `modak` and `pg_duckdb` extensions from
  the release tarballs. [Installation](../getting-started/installation.md)
  lists the files and the postgresql.conf settings.
- `shared_preload_libraries = 'pg_duckdb, modak'` needs a restart, so put it
  in the cluster spec before first boot rather than patching it in later.
- The rest matches any other production deployment: roles, TLS, and WAL
  limits are covered in [Production deployment](production.md).
