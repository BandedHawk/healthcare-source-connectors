package com.sdh.healthcare.connector.file.demo;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Prints the connector's OpenTelemetry output to the console for the demo (design §12). The demo wraps these in the
 * production filters ({@code AllowListSpanExporter}, {@code AuditOnlyLogRecordExporter}), so what's printed is exactly
 * what a collector would receive: allow-listed attributes only, no filenames or record content.
 */
final class ConsoleTelemetry {
    static final PrintStream OUT = System.out;
    static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private ConsoleTelemetry() {}

    static String attrs(Attributes a) {
        Map<String, String> sorted = new TreeMap<>();
        a.forEach((k, v) -> sorted.put(k.getKey(), String.valueOf(v)));
        return sorted.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(" "));
    }

    static String ms(long nanos) {
        return nanos >= 1_000_000_000L ? String.format(Locale.ROOT, "%.2f s", nanos / 1e9)
                : String.format(Locale.ROOT, "%d ms", nanos / 1_000_000);
    }

    /**
     * Traces as trees. Spans are held until their trace's root span ends (it ends last), then the whole trace is
     * printed: one tree per file, showing where its time went.
     */
    static final class SpanTrees implements SpanExporter {
        private final Map<String, List<SpanData>> pending = new ConcurrentHashMap<>();

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            for (SpanData s : spans) {
                pending.computeIfAbsent(s.getTraceId(), t -> new ArrayList<>()).add(s);
                if (!s.getParentSpanContext().isValid()) {
                    print(pending.remove(s.getTraceId()), s);
                }
            }
            return CompletableResultCode.ofSuccess();
        }

        private synchronized void print(List<SpanData> trace, SpanData root) {
            Map<String, List<SpanData>> children = new LinkedHashMap<>();
            for (SpanData s : trace) {
                if (s != root) {
                    children.computeIfAbsent(s.getParentSpanId(), p -> new ArrayList<>()).add(s);
                }
            }
            children.values().forEach(l -> l.sort(Comparator.comparingLong(SpanData::getStartEpochNanos)));
            StringBuilder b = new StringBuilder();
            b.append("\n[otel trace] ").append(root.getTraceId()).append(", ").append(trace.size()).append(" spans\n");
            line(b, root, "", "", children);
            OUT.print(b);
            OUT.flush();
        }

        private void line(StringBuilder b, SpanData s, String lead, String branch, Map<String, List<SpanData>> kids) {
            b.append("  ").append(lead).append(branch).append(s.getName()).append("  ")
                    .append(ms(s.getEndEpochNanos() - s.getStartEpochNanos()));
            if (s.getStatus().getStatusCode() == StatusCode.ERROR) {
                b.append("  ERROR");
            }
            String a = attrs(s.getAttributes());
            if (!a.isEmpty()) {
                b.append("  ").append(a);
            }
            b.append('\n');
            List<SpanData> cs = kids.getOrDefault(s.getSpanId(), List.of());
            String next = lead + (branch.isEmpty() ? "" : branch.startsWith("├") ? "│  " : "   ");
            for (int i = 0; i < cs.size(); i++) {
                line(b, cs.get(i), next, i == cs.size() - 1 ? "└─ " : "├─ ", kids);
            }
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }

    /** Audit events as OpenTelemetry log records, one line each. */
    static final class LogLines implements LogRecordExporter {
        @Override
        public CompletableResultCode export(Collection<LogRecordData> logs) {
            for (LogRecordData l : logs) {
                // Audit events are emitted without a timestamp; the SDK records when it observed them.
                long at = l.getTimestampEpochNanos() != 0 ? l.getTimestampEpochNanos() : l.getObservedTimestampEpochNanos();
                OUT.println("[otel log]   " + TIME.format(Instant.ofEpochSecond(0, at)) + "  "
                        + l.getEventName() + "  " + attrs(l.getAttributes()));
            }
            OUT.flush();
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }

    /** The connector's own metrics ({@code sdh.*}), as running totals, each time the reader collects them. */
    static final class MetricTable implements MetricExporter {
        @Override
        public CompletableResultCode export(Collection<MetricData> metrics) {
            List<MetricData> ours = metrics.stream().filter(m -> m.getName().startsWith("sdh."))
                    .sorted(Comparator.comparing(MetricData::getName)).toList();
            if (ours.isEmpty()) {
                return CompletableResultCode.ofSuccess();
            }
            StringBuilder b = new StringBuilder("\n[otel metrics] " + TIME.format(Instant.now()) + "\n");
            for (MetricData m : ours) {
                for (PointData p : points(m)) {
                    b.append(String.format(Locale.ROOT, "  %-34s %-26s %s%n", m.getName(), value(p, m.getUnit()),
                            attrs(p.getAttributes())));
                }
            }
            OUT.print(b);
            OUT.flush();
            return CompletableResultCode.ofSuccess();
        }

        static Collection<? extends PointData> points(MetricData m) {
            return switch (m.getType()) {
                case LONG_SUM -> m.getLongSumData().getPoints();
                case DOUBLE_SUM -> m.getDoubleSumData().getPoints();
                case LONG_GAUGE -> m.getLongGaugeData().getPoints();
                case DOUBLE_GAUGE -> m.getDoubleGaugeData().getPoints();
                case HISTOGRAM -> m.getHistogramData().getPoints();
                default -> List.of();
            };
        }

        static String value(PointData p, String unit) {
            String u = unit == null || unit.isEmpty() || unit.equals("1") || unit.startsWith("{") ? "" : " " + unit;
            return switch (p) {
                case io.opentelemetry.sdk.metrics.data.LongPointData l -> l.getValue() + u;
                case io.opentelemetry.sdk.metrics.data.DoublePointData d ->
                        String.format(Locale.ROOT, "%.3f%s", d.getValue(), u);
                case HistogramPointData h -> String.format(Locale.ROOT, "n=%d avg=%.3f%s max=%.3f", h.getCount(),
                        h.getCount() == 0 ? 0 : h.getSum() / h.getCount(), u, h.hasMax() ? h.getMax() : 0);
                default -> "?";
            };
        }

        @Override
        public AggregationTemporality getAggregationTemporality(InstrumentType type) {
            return AggregationTemporality.CUMULATIVE;
        }

        @Override
        public MemoryMode getMemoryMode() {
            return MemoryMode.IMMUTABLE_DATA;
        }

        @Override
        public CompletableResultCode flush() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }
    }
}
