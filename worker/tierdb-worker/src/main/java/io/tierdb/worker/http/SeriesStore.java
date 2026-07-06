package io.tierdb.worker.http;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory time series for the console, one bounded ring of (epoch-second,
 * value) points per series key, fed by the status sweep. ~11h of history at
 * the default 10s cycle, no external metrics store required.
 */
public final class SeriesStore {

    public static final int MAX_POINTS = 4096;

    private record Point(long ts, double value) {}

    private final Map<String, ArrayDeque<Point>> series = new ConcurrentSkipListMap<>();

    public void record(String key, double value) {
        long now = System.currentTimeMillis() / 1000;
        ArrayDeque<Point> ring = series.computeIfAbsent(key, k -> new ArrayDeque<>());
        synchronized (ring) {
            Point last = ring.peekLast();
            if (last != null && last.ts() == now) {
                ring.pollLast();
            }
            ring.addLast(new Point(now, value));
            while (ring.size() > MAX_POINTS) {
                ring.pollFirst();
            }
        }
    }

    public Map<String, double[][]> snapshot() {
        Map<String, double[][]> out = new TreeMap<>();
        series.forEach((key, ring) -> {
            synchronized (ring) {
                double[][] points = new double[ring.size()][];
                int i = 0;
                for (Point p : ring) {
                    points[i++] = new double[] {p.ts(), p.value()};
                }
                out.put(key, points);
            }
        });
        return out;
    }
}
