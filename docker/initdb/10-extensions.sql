CREATE EXTENSION IF NOT EXISTS pg_duckdb;
CREATE EXTENSION tierdb;

SELECT duckdb.install_extension('httpfs');
SELECT duckdb.install_extension('iceberg');

ALTER SYSTEM SET duckdb.max_workers_per_postgres_scan = 0;

ALTER SYSTEM SET duckdb.unsafe_allow_execution_inside_functions = 'on';

ALTER SYSTEM SET shared_preload_libraries = 'pg_duckdb', 'tierdb';

ALTER SYSTEM SET wal_level = 'logical';
