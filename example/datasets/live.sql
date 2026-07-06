CREATE TABLE public.trip_events_live (
    id         bigint NOT NULL,
    event_time bigint NOT NULL,
    val        text   NOT NULL
) PARTITION BY RANGE (event_time);

CREATE TABLE public.gps_pings_live (
    id      bigint NOT NULL,
    ts      timestamptz NOT NULL,
    lat     numeric(9,6) NOT NULL,
    lon     numeric(9,6) NOT NULL,
    celsius double precision NOT NULL
) PARTITION BY RANGE (ts);

DO $$
DECLARE
    now_s bigint := extract(epoch FROM now())::bigint;
    lo    bigint := (now_s - 24 * 3600) / 3600 * 3600;
BEGIN
    WHILE lo < now_s + 2 * 3600 LOOP
        EXECUTE format('CREATE TABLE public.trip_events_live_%s PARTITION OF public.trip_events_live
                        FOR VALUES FROM (%s) TO (%s)', lo, lo, lo + 3600);
        EXECUTE format('CREATE TABLE public.gps_pings_live_%s PARTITION OF public.gps_pings_live
                        FOR VALUES FROM (%L) TO (%L)',
                       lo, to_timestamp(lo), to_timestamp(lo + 3600));
        lo := lo + 3600;
    END LOOP;
END $$;

INSERT INTO public.trip_events_live
SELECT t * 10, t, 'event-' || (t % 7)
FROM generate_series(
    (extract(epoch FROM now())::bigint - 24 * 3600) / 3600 * 3600,
    extract(epoch FROM now())::bigint - 1, 2) t;

INSERT INTO public.gps_pings_live
SELECT t * 10, to_timestamp(t),
       37.7749 + 0.01 * sin(t / 1800.0), -122.4194 + 0.01 * cos(t / 1800.0),
       20 + 10 * sin(t / 3600.0) + random()
FROM generate_series(
    (extract(epoch FROM now())::bigint - 24 * 3600) / 3600 * 3600,
    extract(epoch FROM now())::bigint - 1, 2) t;
