INSERT INTO public.tire_pressure_logs
SELECT g, g, round((30 + (g % 5) + random())::numeric, 2) FROM generate_series(1, 20000) g;
