/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Stands in for an OpenTelemetry collector (design §15.3): both apps
 * export their traces, audit events and metrics to it over OTLP/HTTP, as
 * they would to a real one. What arrives has already been through the
 * apps' own PHI filters, so it is exactly what a collector would get.
 * <p>
 * Everything is kept for later review under {@code telemetry/<service>/}
 * as OTLP JSON, one export request a line ({@code traces.jsonl},
 * {@code logs.jsonl}, {@code metrics.jsonl}), with trace and span IDs in
 * hex as the OTLP JSON encoding has them. It is also printed as it comes:
 * each trace as a tree once its root span arrives ({@code [otel trace]}),
 * each audit event ({@code [otel log]}), and the {@code sdh.*} metrics
 * ({@code [otel metrics]}). The demo waits on the audit events to follow
 * each item through the pipeline.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class OtlpReceiver implements AutoCloseable
{

    /**
     * An audit event, as an app exported it.
     *
     * @param service    the app, from its resource's {@code service.name}
     * @param nanos      when, in nanoseconds since the epoch
     * @param name       the event, such as {@code item.landed}
     * @param severity   {@code INFO}, {@code WARN} or {@code ERROR}
     * @param attributes its attributes, without the {@code sdh.} prefix
     * @param traceId    the trace it was part of, in hex; empty if none
     */
    record AuditEvent(String service, long nanos, String name,
            String severity, Map<String, String> attributes, String traceId)
    {

        /**
         * An attribute.
         *
         * @param key its name, without the {@code sdh.} prefix
         * @return its value; null if it hasn't one
         */
        String get(String key)
        {
            return attributes.get(key);
        }
    }

    /**
     * A finished span, as an app exported it.
     *
     * @param service    the app
     * @param traceId    its trace, in hex
     * @param spanId     its ID, in hex
     * @param parentId   its parent's ID, in hex; empty for a root
     * @param name       its name, such as {@code ingest.item}
     * @param start      when it started, in nanoseconds since the epoch
     * @param end        when it ended
     * @param error      whether it ended in error
     * @param attributes its attributes, without the {@code sdh.} prefix
     */
    record SpanRecord(String service, String traceId, String spanId,
            String parentId, String name, long start, long end,
            boolean error, Map<String, String> attributes)
    {

        /**
         * How long it took.
         *
         * @return nanoseconds
         */
        long nanos()
        {
            return end - start;
        }
    }

    /** Clock times on the console. */
    static final DateTimeFormatter TIME = DateTimeFormatter
            .ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    /** Reads and rewrites the JSON lines. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The HTTP server. */
    private final HttpServer server;

    /** Where the telemetry files go. */
    private final Path directory;

    /** Every audit event so far, in arrival order. */
    private final List<AuditEvent> events = new ArrayList<>();

    /** Every span so far, in arrival order. */
    private final List<SpanRecord> spans = new ArrayList<>();

    /** Spans of traces whose root hasn't arrived yet, by trace. */
    private final Map<String, List<SpanRecord>> pending =
            new LinkedHashMap<>();

    /**
     * Starts the receiver on a free local port.
     *
     * @param directory where to keep the telemetry
     * @throws IOException if the port can't be opened
     */
    OtlpReceiver(Path directory) throws IOException
    {
        this.directory = Files.createDirectories(directory);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        // One thread, so exports are handled in the order they come.
        server.setExecutor(Executors.newSingleThreadExecutor());
        server.createContext("/v1/traces", exchange -> handle(exchange,
                this::traces));
        server.createContext("/v1/logs", exchange -> handle(exchange,
                this::logs));
        server.createContext("/v1/metrics", exchange -> handle(exchange,
                this::metrics));
        server.start();
    }

    /**
     * The endpoint to give the apps as {@code sdh.telemetry.otlp.endpoint}.
     *
     * @return the base URL
     */
    String endpoint()
    {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    /**
     * Where the telemetry is kept.
     *
     * @return the directory
     */
    Path directory()
    {
        return directory;
    }

    /** Handles one signal's export request body. */
    @FunctionalInterface
    private interface Handler
    {

        /**
         * Handles a request.
         *
         * @param body the protobuf body
         * @return the protobuf response
         * @throws IOException if it can't be read or kept
         */
        Message handle(byte[] body) throws IOException;
    }

    /**
     * Reads an export request, handles it and answers.
     *
     * @param exchange the HTTP exchange
     * @param handler  what to do with the body
     * @throws IOException if the exchange fails
     */
    private void handle(HttpExchange exchange, Handler handler)
            throws IOException
    {
        try (exchange)
        {
            InputStream input = exchange.getRequestBody();
            if ("gzip".equalsIgnoreCase(exchange.getRequestHeaders()
                    .getFirst("Content-Encoding")))
            {
                input = new GZIPInputStream(input);
            }
            byte[] response;
            try
            {
                response = handler.handle(input.readAllBytes()).toByteArray();
            }
            catch (IOException | RuntimeException failure)
            {
                System.out.println("[otel] couldn't handle an export: "
                        + failure);
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            exchange.getResponseHeaders().set("Content-Type",
                    "application/x-protobuf");
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream output = exchange.getResponseBody())
            {
                output.write(response);
            }
        }
    }

    /**
     * Keeps and prints a trace export.
     *
     * @param body the request
     * @return the response
     * @throws IOException if it can't be kept
     */
    private Message traces(byte[] body) throws IOException
    {
        ExportTraceServiceRequest request =
                ExportTraceServiceRequest.parseFrom(body);
        for (ResourceSpans resource : request.getResourceSpansList())
        {
            String service = service(resource.getResource());
            keep(service, "traces", request);
            for (ScopeSpans scope : resource.getScopeSpansList())
            {
                for (Span span : scope.getSpansList())
                {
                    span(new SpanRecord(service, hex(span.getTraceId()),
                            hex(span.getSpanId()),
                            hex(span.getParentSpanId()), span.getName(),
                            span.getStartTimeUnixNano(),
                            span.getEndTimeUnixNano(),
                            span.getStatus().getCode()
                                    == Status.StatusCode.STATUS_CODE_ERROR,
                            attributes(span.getAttributesList())));
                }
            }
        }
        return ExportTraceServiceResponse.getDefaultInstance();
    }

    /**
     * Records a span, and prints its trace once the root has come: the
     * root ends last, so its trace is complete then.
     *
     * @param span the span
     */
    private synchronized void span(SpanRecord span)
    {
        spans.add(span);
        pending.computeIfAbsent(span.traceId(), trace -> new ArrayList<>())
                .add(span);
        if (span.parentId().isEmpty())
        {
            System.out.print(tree(pending.remove(span.traceId()), span));
            System.out.flush();
        }
    }

    /**
     * A trace as an indented tree, with each span's duration and
     * attributes.
     *
     * @param trace its spans
     * @param root  its root span
     * @return the text
     */
    static String tree(List<SpanRecord> trace, SpanRecord root)
    {
        Map<String, List<SpanRecord>> children = new LinkedHashMap<>();
        for (SpanRecord span : trace)
        {
            if (span != root)
            {
                children.computeIfAbsent(span.parentId(),
                        parent -> new ArrayList<>()).add(span);
            }
        }
        children.values().forEach(siblings -> siblings.sort(
                Comparator.comparingLong(SpanRecord::start)));
        StringBuilder text = new StringBuilder();
        text.append("\n[otel trace] ").append(root.service()).append(' ')
                .append(root.traceId()).append(", ").append(trace.size())
                .append(" spans\n");
        line(text, root, "", "", children);
        return text.toString();
    }

    /**
     * Appends a span's line, then its children's.
     *
     * @param text     where to write
     * @param span     the span
     * @param lead     the indent from the levels above
     * @param branch   this span's branch mark
     * @param children each span's children, by parent ID
     */
    private static void line(StringBuilder text, SpanRecord span, String lead,
            String branch, Map<String, List<SpanRecord>> children)
    {
        text.append("  ").append(lead).append(branch).append(span.name())
                .append("  ").append(duration(span.nanos()));
        if (span.error())
        {
            text.append("  ERROR");
        }
        if (!span.attributes().isEmpty())
        {
            text.append("  ").append(text(span.attributes()));
        }
        text.append('\n');
        List<SpanRecord> own =
                children.getOrDefault(span.spanId(), List.of());
        String next = lead + (branch.isEmpty() ? ""
                : branch.startsWith("├") ? "│  " : "   ");
        for (int index = 0; index < own.size(); index++)
        {
            line(text, own.get(index), next,
                    index == own.size() - 1 ? "└─ " : "├─ ", children);
        }
    }

    /**
     * Keeps and prints a log export: the apps export audit events only.
     *
     * @param body the request
     * @return the response
     * @throws IOException if it can't be kept
     */
    private Message logs(byte[] body) throws IOException
    {
        ExportLogsServiceRequest request =
                ExportLogsServiceRequest.parseFrom(body);
        for (ResourceLogs resource : request.getResourceLogsList())
        {
            String service = service(resource.getResource());
            keep(service, "logs", request);
            for (ScopeLogs scope : resource.getScopeLogsList())
            {
                for (LogRecord log : scope.getLogRecordsList())
                {
                    // Audit events are emitted without a timestamp; the
                    // SDK records when it observed them.
                    long at = log.getTimeUnixNano() != 0
                            ? log.getTimeUnixNano()
                            : log.getObservedTimeUnixNano();
                    String name = log.getEventName()
                            .replaceFirst("^sdh\\.audit\\.", "");
                    event(new AuditEvent(service, at, name,
                            log.getSeverityText().isEmpty()
                                    ? log.getSeverityNumber().name()
                                            .replace("SEVERITY_NUMBER_", "")
                                    : log.getSeverityText(),
                            attributes(log.getAttributesList()),
                            hex(log.getTraceId())));
                }
            }
        }
        return ExportLogsServiceResponse.getDefaultInstance();
    }

    /**
     * Records an audit event and prints it.
     *
     * @param event the event
     */
    private synchronized void event(AuditEvent event)
    {
        events.add(event);
        notifyAll();
        System.out.println(String.format(Locale.ROOT,
                "[otel log]   %s  %-17s %-5s %s  %s",
                TIME.format(Instant.ofEpochSecond(0, event.nanos())),
                event.service(), event.severity(), event.name(),
                text(event.attributes())));
        System.out.flush();
    }

    /**
     * Keeps and prints a metrics export: the {@code sdh.*} metrics, as
     * running totals.
     *
     * @param body the request
     * @return the response
     * @throws IOException if it can't be kept
     */
    private Message metrics(byte[] body) throws IOException
    {
        ExportMetricsServiceRequest request =
                ExportMetricsServiceRequest.parseFrom(body);
        for (ResourceMetrics resource : request.getResourceMetricsList())
        {
            String service = service(resource.getResource());
            keep(service, "metrics", request);
            List<Metric> ours = new ArrayList<>();
            for (ScopeMetrics scope : resource.getScopeMetricsList())
            {
                scope.getMetricsList().stream()
                        .filter(metric -> metric.getName().startsWith("sdh."))
                        .forEach(ours::add);
            }
            if (ours.isEmpty())
            {
                continue;
            }
            ours.sort(Comparator.comparing(Metric::getName));
            StringBuilder text = new StringBuilder("\n[otel metrics] "
                    + service + " " + TIME.format(Instant.now()) + "\n");
            for (Metric metric : ours)
            {
                for (String point : points(metric))
                {
                    text.append(String.format(Locale.ROOT, "  %-34s %s%n",
                            metric.getName(), point));
                }
            }
            System.out.print(text);
            System.out.flush();
        }
        return ExportMetricsServiceResponse.getDefaultInstance();
    }

    /**
     * A metric's points, each as its value and attributes.
     *
     * @param metric the metric
     * @return the lines
     */
    static List<String> points(Metric metric)
    {
        String unit = metric.getUnit().isEmpty() || metric.getUnit()
                .equals("1") || metric.getUnit().startsWith("{") ? ""
                        : " " + metric.getUnit();
        List<String> lines = new ArrayList<>();
        List<NumberDataPoint> numbers = switch (metric.getDataCase())
        {
            case SUM -> metric.getSum().getDataPointsList();
            case GAUGE -> metric.getGauge().getDataPointsList();
            default -> List.of();
        };
        for (NumberDataPoint point : numbers)
        {
            String value = point.getValueCase()
                    == NumberDataPoint.ValueCase.AS_INT
                            ? Long.toString(point.getAsInt())
                            : String.format(Locale.ROOT, "%.3f",
                                    point.getAsDouble());
            lines.add(String.format(Locale.ROOT, "%-26s %s", value + unit,
                    text(attributes(point.getAttributesList()))));
        }
        if (metric.hasHistogram())
        {
            for (HistogramDataPoint point :
                    metric.getHistogram().getDataPointsList())
            {
                lines.add(String.format(Locale.ROOT, "%-26s %s",
                        String.format(Locale.ROOT, "n=%d avg=%.3f%s max=%.3f",
                                point.getCount(), point.getCount() == 0 ? 0
                                        : point.getSum() / point.getCount(),
                                unit, point.getMax()),
                        text(attributes(point.getAttributesList()))));
            }
        }
        return lines;
    }

    /**
     * Waits for an audit event.
     *
     * @param service the app it must come from
     * @param name    the event
     * @param match   which of them; any if it matches everything
     * @param from    how many events to pass over first: those that came
     *                before the step the demo is waiting on
     * @param timeout how long to wait at most
     * @return the event
     * @throws InterruptedException if interrupted
     * @throws IllegalStateException if it doesn't come in time
     */
    AuditEvent await(String service, String name,
            Predicate<AuditEvent> match, int from, Duration timeout)
            throws InterruptedException
    {
        return await(service, Set.of(name), match, from, timeout);
    }

    /**
     * Waits for the first of several audit events.
     *
     * @param service the app it must come from
     * @param names   the events, any of which will do
     * @param match   which of them; any if it matches everything
     * @param from    how many events to pass over first
     * @param timeout how long to wait at most
     * @return the event
     * @throws InterruptedException if interrupted
     * @throws IllegalStateException if none comes in time
     */
    synchronized AuditEvent await(String service, Set<String> names,
            Predicate<AuditEvent> match, int from, Duration timeout)
            throws InterruptedException
    {
        long until = System.currentTimeMillis() + timeout.toMillis();
        int seen = from;
        while (true)
        {
            for (; seen < events.size(); seen++)
            {
                AuditEvent event = events.get(seen);
                if (event.service().equals(service)
                        && names.contains(event.name()) && match.test(event))
                {
                    return event;
                }
            }
            long left = until - System.currentTimeMillis();
            if (left <= 0)
            {
                throw new IllegalStateException("No " + names + " from "
                        + service + " within " + timeout.toSeconds() + " s");
            }
            wait(Math.min(left, 500));
        }
    }

    /**
     * How many audit events have come so far: where the next wait starts.
     *
     * @return the count
     */
    synchronized int eventCount()
    {
        return events.size();
    }

    /**
     * The audit events so far.
     *
     * @return a copy, in arrival order
     */
    synchronized List<AuditEvent> events()
    {
        return List.copyOf(events);
    }

    /**
     * The spans so far.
     *
     * @return a copy, in arrival order
     */
    synchronized List<SpanRecord> spans()
    {
        return List.copyOf(spans);
    }

    /**
     * Appends an export request to its file, as OTLP JSON with hex IDs.
     *
     * @param service the app
     * @param signal  {@code traces}, {@code logs} or {@code metrics}
     * @param request the request
     * @throws IOException if it can't be written
     */
    private synchronized void keep(String service, String signal,
            Message request) throws IOException
    {
        JsonNode json = JSON.readTree(JsonFormat.printer()
                .omittingInsignificantWhitespace().print(request));
        hexIds(json);
        Path file = Files.createDirectories(directory.resolve(service))
                .resolve(signal + ".jsonl");
        Files.writeString(file, JSON.writeValueAsString(json) + "\n", UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * Rewrites trace and span IDs from protobuf's base64 to the hex that
     * OTLP JSON uses, so that they match the console and other tools.
     *
     * @param node the JSON to rewrite, in place
     */
    static void hexIds(JsonNode node)
    {
        if (node instanceof ObjectNode object)
        {
            for (Iterator<Map.Entry<String, JsonNode>> fields =
                    object.fields(); fields.hasNext();)
            {
                Map.Entry<String, JsonNode> field = fields.next();
                if (field.getValue().isTextual() && (field.getKey()
                        .equals("traceId") || field.getKey().equals("spanId")
                        || field.getKey().equals("parentSpanId")))
                {
                    field.setValue(object.textNode(HexFormat.of().formatHex(
                            Base64.getDecoder().decode(
                                    field.getValue().asText()))));
                }
                else
                {
                    hexIds(field.getValue());
                }
            }
        }
        else if (node.isArray())
        {
            node.forEach(OtlpReceiver::hexIds);
        }
    }

    /**
     * The app a resource is, from its {@code service.name}.
     *
     * @param resource the resource
     * @return the name; {@code unknown} if it has none
     */
    static String service(Resource resource)
    {
        return resource.getAttributesList().stream()
                .filter(attribute -> attribute.getKey().equals("service.name"))
                .map(attribute -> value(attribute.getValue()))
                .findFirst().orElse("unknown");
    }

    /**
     * Attributes as text, sorted by name, without the {@code sdh.} prefix.
     *
     * @param list the attributes
     * @return name to value
     */
    static Map<String, String> attributes(List<KeyValue> list)
    {
        Map<String, String> sorted = new TreeMap<>();
        for (KeyValue attribute : list)
        {
            sorted.put(attribute.getKey().replaceFirst("^sdh\\.", ""),
                    value(attribute.getValue()));
        }
        return sorted;
    }

    /**
     * An attribute value as text.
     *
     * @param value the value
     * @return the text
     */
    static String value(AnyValue value)
    {
        return switch (value.getValueCase())
        {
            case STRING_VALUE -> value.getStringValue();
            case INT_VALUE -> Long.toString(value.getIntValue());
            case DOUBLE_VALUE -> Double.toString(value.getDoubleValue());
            case BOOL_VALUE -> Boolean.toString(value.getBoolValue());
            default -> value.toString().strip();
        };
    }

    /**
     * Attributes as {@code name=value} pairs.
     *
     * @param attributes the attributes
     * @return the text
     */
    static String text(Map<String, String> attributes)
    {
        return attributes.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(" "));
    }

    /**
     * An ID in hex.
     *
     * @param id the ID's bytes
     * @return the hex; empty if it has none
     */
    static String hex(ByteString id)
    {
        return HexFormat.of().formatHex(id.toByteArray());
    }

    /**
     * A duration, for people.
     *
     * @param nanos nanoseconds
     * @return such as {@code 12 ms} or {@code 1.25 s}
     */
    static String duration(long nanos)
    {
        return nanos >= 1_000_000_000L
                ? String.format(Locale.ROOT, "%.2f s", nanos / 1e9)
                : String.format(Locale.ROOT, "%d ms", nanos / 1_000_000);
    }

    /** Stops the receiver, after a moment for the last exports. */
    @Override
    public void close()
    {
        server.stop(1);
    }
}
