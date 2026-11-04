/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.export.MemoryMode;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
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
 * Prints the connector's OpenTelemetry output to the console for the demo
 * (design §12). The demo wraps these in the production filters
 * ({@code AllowListSpanExporter}, {@code AuditOnlyLogRecordExporter}), so
 * what's printed is exactly what a collector would receive: allow-listed
 * attributes only, no filenames or record content.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ConsoleTelemetry
{

    /** Where everything is printed. */
    static final PrintStream OUT = System.out;

    /** Times of day, in the local zone. */
    static final DateTimeFormatter TIME = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    /** Not instantiable. */
    private ConsoleTelemetry()
    {
    }

    /**
     * Attributes as {@code key=value} pairs, sorted by key.
     *
     * @param attributes the attributes
     * @return the pairs, separated by spaces
     */
    static String attrs(Attributes attributes)
    {
        Map<String, String> sorted = new TreeMap<>();
        attributes.forEach((key, value) ->
                sorted.put(key.getKey(), String.valueOf(value)));
        return sorted.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(" "));
    }

    /**
     * A duration in milliseconds, or seconds from one second up.
     *
     * @param nanos the duration in nanoseconds
     * @return the text, such as {@code 12 ms} or {@code 1.50 s}
     */
    static String ms(long nanos)
    {
        return nanos >= 1_000_000_000L
                ? String.format(Locale.ROOT, "%.2f s", nanos / 1e9)
                : String.format(Locale.ROOT, "%d ms", nanos / 1_000_000);
    }

    /**
     * Traces as trees. Spans are held until their trace's root span ends
     * (it ends last), then the whole trace is printed: one tree per file,
     * showing where its time went.
     */
    static final class SpanTrees implements SpanExporter
    {

        /** Ended spans of unfinished traces, by trace ID. */
        private final Map<String, List<SpanData>> pending =
                new ConcurrentHashMap<>();

        /** Created by the demo. */
        SpanTrees()
        {
        }

        /**
         * Holds spans, and prints a trace once its root ends.
         *
         * @param spans the ended spans
         * @return success
         */
        @Override
        public CompletableResultCode export(Collection<SpanData> spans)
        {
            for (SpanData span : spans)
            {
                pending.computeIfAbsent(span.getTraceId(),
                        trace -> new ArrayList<>()).add(span);
                if (!span.getParentSpanContext().isValid())
                {
                    print(pending.remove(span.getTraceId()), span);
                }
            }
            return CompletableResultCode.ofSuccess();
        }

        /**
         * Prints a trace as a tree.
         *
         * @param trace every span of the trace
         * @param root the root span
         */
        private synchronized void print(List<SpanData> trace, SpanData root)
        {
            Map<String, List<SpanData>> children = new LinkedHashMap<>();
            for (SpanData span : trace)
            {
                if (span != root)
                {
                    children.computeIfAbsent(span.getParentSpanId(),
                            parent -> new ArrayList<>()).add(span);
                }
            }
            children.values().forEach(siblings -> siblings.sort(
                    Comparator.comparingLong(SpanData::getStartEpochNanos)));
            StringBuilder text = new StringBuilder();
            text.append("\n[otel trace] ").append(root.getTraceId())
                    .append(", ").append(trace.size()).append(" spans\n");
            line(text, root, "", "", children);
            OUT.print(text);
            OUT.flush();
        }

        /**
         * Appends a span's line and, below it, its children's.
         *
         * @param text where to append
         * @param span the span
         * @param lead the tree lines inherited from its ancestors
         * @param branch the span's own branch, or empty for the root
         * @param children each span's children, by parent span ID
         */
        private void line(StringBuilder text, SpanData span, String lead,
                String branch, Map<String, List<SpanData>> children)
        {
            text.append("  ").append(lead).append(branch)
                    .append(span.getName()).append("  ")
                    .append(ms(span.getEndEpochNanos()
                            - span.getStartEpochNanos()));
            if (span.getStatus().getStatusCode() == StatusCode.ERROR)
            {
                text.append("  ERROR");
            }
            String attributes = attrs(span.getAttributes());
            if (!attributes.isEmpty())
            {
                text.append("  ").append(attributes);
            }
            text.append('\n');
            List<SpanData> own =
                    children.getOrDefault(span.getSpanId(), List.of());
            String next = lead + (branch.isEmpty() ? ""
                    : branch.startsWith("├") ? "│  " : "   ");
            for (int index = 0; index < own.size(); index++)
            {
                line(text, own.get(index), next,
                        index == own.size() - 1 ? "└─ " : "├─ ", children);
            }
        }

        /**
         * Nothing to flush.
         *
         * @return success
         */
        @Override
        public CompletableResultCode flush()
        {
            return CompletableResultCode.ofSuccess();
        }

        /**
         * Nothing to shut down.
         *
         * @return success
         */
        @Override
        public CompletableResultCode shutdown()
        {
            return CompletableResultCode.ofSuccess();
        }
    }

    /** Audit events as OpenTelemetry log records, one line each. */
    static final class LogLines implements LogRecordExporter
    {

        /** Created by the demo. */
        LogLines()
        {
        }

        /**
         * Prints each record.
         *
         * @param logs the records
         * @return success
         */
        @Override
        public CompletableResultCode export(Collection<LogRecordData> logs)
        {
            for (LogRecordData log : logs)
            {
                // Audit events are emitted without a timestamp; the SDK
                // records when it observed them.
                long at = log.getTimestampEpochNanos() != 0
                        ? log.getTimestampEpochNanos()
                        : log.getObservedTimestampEpochNanos();
                OUT.println("[otel log]   "
                        + TIME.format(Instant.ofEpochSecond(0, at)) + "  "
                        + log.getEventName() + "  "
                        + attrs(log.getAttributes()));
            }
            OUT.flush();
            return CompletableResultCode.ofSuccess();
        }

        /**
         * Nothing to flush.
         *
         * @return success
         */
        @Override
        public CompletableResultCode flush()
        {
            return CompletableResultCode.ofSuccess();
        }

        /**
         * Nothing to shut down.
         *
         * @return success
         */
        @Override
        public CompletableResultCode shutdown()
        {
            return CompletableResultCode.ofSuccess();
        }
    }

    /**
     * The connector's own metrics ({@code sdh.*}), as running totals, each
     * time the reader collects them.
     */
    static final class MetricTable implements MetricExporter
    {

        /** Created by the demo. */
        MetricTable()
        {
        }

        /**
         * Prints the connector's metrics as a table.
         *
         * @param metrics every metric collected
         * @return success
         */
        @Override
        public CompletableResultCode export(Collection<MetricData> metrics)
        {
            List<MetricData> ours = metrics.stream()
                    .filter(metric -> metric.getName().startsWith("sdh."))
                    .sorted(Comparator.comparing(MetricData::getName))
                    .toList();
            if (ours.isEmpty())
            {
                return CompletableResultCode.ofSuccess();
            }
            StringBuilder text = new StringBuilder(
                    "\n[otel metrics] " + TIME.format(Instant.now()) + "\n");
            for (MetricData metric : ours)
            {
                for (PointData point : points(metric))
                {
                    text.append(String.format(Locale.ROOT,
                            "  %-34s %-26s %s%n", metric.getName(),
                            value(point, metric.getUnit()),
                            attrs(point.getAttributes())));
                }
            }
            OUT.print(text);
            OUT.flush();
            return CompletableResultCode.ofSuccess();
        }

        /**
         * A metric's points, for the types the connector uses.
         *
         * @param metric the metric
         * @return its points, or none for another type
         */
        static Collection<? extends PointData> points(MetricData metric)
        {
            return switch (metric.getType())
            {
                case LONG_SUM -> metric.getLongSumData().getPoints();
                case DOUBLE_SUM -> metric.getDoubleSumData().getPoints();
                case LONG_GAUGE -> metric.getLongGaugeData().getPoints();
                case DOUBLE_GAUGE -> metric.getDoubleGaugeData().getPoints();
                case HISTOGRAM -> metric.getHistogramData().getPoints();
                default -> List.of();
            };
        }

        /**
         * A point's value with its unit; a histogram as count, average and
         * maximum.
         *
         * @param point the point
         * @param unit the metric's unit; {@code 1} and {@code {…}} aren't
         *        shown
         * @return the text
         */
        static String value(PointData point, String unit)
        {
            String shown = unit == null || unit.isEmpty() || unit.equals("1")
                    || unit.startsWith("{") ? "" : " " + unit;
            return switch (point)
            {
                case LongPointData longPoint -> longPoint.getValue() + shown;
                case DoublePointData doublePoint -> String.format(Locale.ROOT,
                        "%.3f%s", doublePoint.getValue(), shown);
                case HistogramPointData histogram -> String.format(
                        Locale.ROOT, "n=%d avg=%.3f%s max=%.3f",
                        histogram.getCount(),
                        histogram.getCount() == 0 ? 0
                                : histogram.getSum() / histogram.getCount(),
                        shown, histogram.hasMax() ? histogram.getMax() : 0);
                default -> "?";
            };
        }

        /**
         * Running totals.
         *
         * @param type the instrument type
         * @return cumulative
         */
        @Override
        public AggregationTemporality getAggregationTemporality(
                InstrumentType type)
        {
            return AggregationTemporality.CUMULATIVE;
        }

        /**
         * The data may be kept between exports.
         *
         * @return immutable data
         */
        @Override
        public MemoryMode getMemoryMode()
        {
            return MemoryMode.IMMUTABLE_DATA;
        }

        /**
         * Nothing to flush.
         *
         * @return success
         */
        @Override
        public CompletableResultCode flush()
        {
            return CompletableResultCode.ofSuccess();
        }

        /**
         * Nothing to shut down.
         *
         * @return success
         */
        @Override
        public CompletableResultCode shutdown()
        {
            return CompletableResultCode.ofSuccess();
        }
    }
}
