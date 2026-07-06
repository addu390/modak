CREATE TABLE public.tire_pressure_logs (
    id  bigint PRIMARY KEY,
    ts  bigint NOT NULL,
    psi numeric(5,2) NOT NULL
);
