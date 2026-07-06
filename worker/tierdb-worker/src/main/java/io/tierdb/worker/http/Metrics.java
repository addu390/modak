package io.tierdb.worker.http;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * A tiny in-process metrics registry rendered in the Prometheus text exposition
 * format. Deliberately dependency-free (no Micrometer): the worker exposes a
 * dozen gauges and counters, nothing that justifies a metrics stack.
 */
public final class Metrics {

    private final Map<String, Double> gauges = new ConcurrentSkipListMap<>();
    private final Map<String, LongAdder> counters = new ConcurrentSkipListMap<>();

    public void gauge(String series, double value) {
        gauges.put(series, value);
    }

    public void increment(String series) {
        add(series, 1);
    }

    public void add(String series, long delta) {
        counters.computeIfAbsent(series, k -> new LongAdder()).add(delta);
    }

    public static String series(String name, String... labelPairs) {
        if (labelPairs.length == 0) {
            return name;
        }
        StringBuilder sb = new StringBuilder(name).append('{');
        for (int i = 0; i < labelPairs.length; i += 2) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(labelPairs[i]).append("=\"")
                    .append(labelPairs[i + 1].replace("\\", "\\\\").replace("\"", "\\\""))
                    .append('"');
        }
        return sb.append('}').toString();
    }

    public String render() {
        StringBuilder sb = new StringBuilder();
        gauges.forEach((series, value) -> {
            sb.append(series).append(' ');
            if (value == Math.floor(value) && !Double.isInfinite(value)) {
                sb.append((long) (double) value);
            } else {
                sb.append(value);
            }
            sb.append('\n');
        });
        counters.forEach((series, value) ->
                sb.append(series).append(' ').append(value.sum()).append('\n'));
        return sb.toString();
    }
}
