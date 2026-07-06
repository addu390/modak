CREATE TABLE public.trip_events (
    id         bigint NOT NULL,
    event_time bigint NOT NULL,
    val        text
) PARTITION BY RANGE (event_time);

CREATE TABLE public.trip_events_p0 PARTITION OF public.trip_events FOR VALUES FROM (0)   TO (100);
CREATE TABLE public.trip_events_p1 PARTITION OF public.trip_events FOR VALUES FROM (100) TO (200);
CREATE TABLE public.trip_events_p2 PARTITION OF public.trip_events FOR VALUES FROM (200) TO (300);
