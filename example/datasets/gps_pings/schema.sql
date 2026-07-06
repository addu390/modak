CREATE TABLE public.gps_pings (
    id      bigint NOT NULL,
    ts      timestamptz NOT NULL,
    lat     numeric(9,6) NOT NULL,
    lon     numeric(9,6) NOT NULL,
    celsius double precision
) PARTITION BY RANGE (ts);

CREATE TABLE public.gps_pings_d1 PARTITION OF public.gps_pings
    FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-01-02 00:00:00+00');
CREATE TABLE public.gps_pings_d2 PARTITION OF public.gps_pings
    FOR VALUES FROM ('2026-01-02 00:00:00+00') TO ('2026-01-03 00:00:00+00');
CREATE TABLE public.gps_pings_d3 PARTITION OF public.gps_pings
    FOR VALUES FROM ('2026-01-03 00:00:00+00') TO ('2026-01-04 00:00:00+00');
