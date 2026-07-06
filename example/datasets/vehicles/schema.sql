CREATE TABLE public.vehicles (
    id        bigint PRIMARY KEY,
    vin       text   NOT NULL,
    status    text,
    active    boolean NOT NULL DEFAULT true,
    last_seen bigint NOT NULL
);
