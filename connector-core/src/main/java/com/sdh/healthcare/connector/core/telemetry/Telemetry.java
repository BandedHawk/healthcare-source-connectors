package com.sdh.healthcare.connector.core.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Traces, metrics and audit events for the connector (design §12), on the OpenTelemetry API. The application installs
 * an SDK with {@link #install}; until then everything is a no-op (tests, libraries).
 *
 * <p>Every attribute goes through {@link Fields}, so only allow-listed keys exist. Never record exception messages or
 * stack traces on spans: use {@link #fail} with an error code.
 *
 * <p>Audit events are OpenTelemetry log records named {@code sdh.audit.<event>}, and are also written to the console
 * log as before.
 */
public final class Telemetry {
    private static final Logger AUDIT_LOG = LoggerFactory.getLogger("sdh.audit");
    private static final String SCOPE = "com.sdh.healthcare.connector";
    private static volatile Telemetry current = new Telemetry(OpenTelemetry.noop());

    private final OpenTelemetry otel;
    private final Tracer tracer;
    private final io.opentelemetry.api.logs.Logger events;
    private final Meter meter;
    private final LongCounter bytesRead;
    private final LongCounter sectionsStored;
    private final LongCounter sectionsPublished;
    private final LongCounter filesCompleted;
    private final LongCounter filesFailed;
    private final DoubleHistogram commitDuration;
    private final DoubleHistogram publishDuration;
    private final AtomicLong filesInProgress = new AtomicLong();
    /** Progress of the file being split, in parts per million (0 when idle). */
    private final AtomicLong progressPpm = new AtomicLong();

    private Telemetry(OpenTelemetry otel) {
        this.otel = otel;
        this.tracer = otel.getTracer(SCOPE);
        this.events = otel.getLogsBridge().get(SCOPE);
        this.meter = otel.getMeter(SCOPE);
        bytesRead = meter.counterBuilder("sdh.bytes.read").setUnit("By")
                .setDescription("Source bytes read, by stage (split, archive)").build();
        sectionsStored = meter.counterBuilder("sdh.sections.stored").setDescription("Sections committed").build();
        sectionsPublished = meter.counterBuilder("sdh.sections.published")
                .setDescription("section.stored messages acknowledged by the broker").build();
        filesCompleted = meter.counterBuilder("sdh.files.completed").build();
        filesFailed = meter.counterBuilder("sdh.files.failed").setDescription("By error code and stage").build();
        commitDuration = meter.histogramBuilder("sdh.commit.duration").setUnit("s").build();
        publishDuration = meter.histogramBuilder("sdh.publish.duration").setUnit("s").build();
        meter.gaugeBuilder("sdh.files.in_progress").ofLongs()
                .buildWithCallback(m -> m.record(filesInProgress.get()));
        meter.gaugeBuilder("sdh.file.progress_ratio").setDescription("Bytes split / file size of the current file")
                .buildWithCallback(m -> m.record(progressPpm.get() / 1_000_000.0));
    }

    /** Installs the application's OpenTelemetry instance. Components look it up when they emit. */
    public static Telemetry install(OpenTelemetry otel) {
        current = new Telemetry(otel);
        return current;
    }

    public static Telemetry get() {
        return current;
    }

    public OpenTelemetry openTelemetry() {
        return otel;
    }

    // ---- traces ----

    /** Starts a span (child of the current context). Make it current with {@code makeCurrent()} and end it. */
    public Span span(String name, Fields fields) {
        return tracer.spanBuilder(name).setAllAttributes(fields.toAttributes()).startSpan();
    }

    /** Marks a span failed with a code: no message or stack trace, which could carry PHI. */
    public static void fail(Span span, ErrorCode code) {
        if (code != null) {
            span.setAttribute(Attr.ERROR_CODE.key(), code.name());
        }
        span.setStatus(StatusCode.ERROR);
    }

    public static void fail(Span span, Throwable t) {
        span.setAttribute(Attr.REASON.key(), t.getClass().getSimpleName());
        span.setStatus(StatusCode.ERROR);
    }

    // ---- audit events ----

    public void audit(String event, Fields fields) {
        audit(event, Severity.INFO, fields);
    }

    public void auditWarn(String event, Fields fields) {
        audit(event, Severity.WARN, fields);
    }

    public void auditError(String event, Fields fields) {
        audit(event, Severity.ERROR, fields);
    }

    private void audit(String event, Severity severity, Fields fields) {
        events.logRecordBuilder()
                .setEventName("sdh.audit." + event)
                .setSeverity(severity)
                .setBody(event)
                .setAllAttributes(fields.toAttributes())
                .emit();
        String line = event + fields.toLogString();
        switch (severity) {
            case ERROR -> AUDIT_LOG.error(line);
            case WARN -> AUDIT_LOG.warn(line);
            default -> AUDIT_LOG.info(line);
        }
    }

    // ---- metrics ----

    public void bytesRead(long n, String stage) {
        bytesRead.add(n, Fields.of(Attr.STAGE, stage).toAttributes());
    }

    public void sectionsStored(long n, String format) {
        sectionsStored.add(n, Fields.of(Attr.FORMAT, format).toAttributes());
    }

    public void sectionsPublished(long n) {
        sectionsPublished.add(n);
    }

    public void fileCompleted() {
        filesCompleted.add(1);
    }

    public void fileFailed(ErrorCode code, String stage) {
        filesFailed.add(1, Fields.of(Attr.ERROR_CODE, code).with(Attr.STAGE, stage).toAttributes());
    }

    public void commitDuration(long nanos, String table) {
        commitDuration.record(nanos / 1e9, Fields.of(Attr.TABLE, table).toAttributes());
    }

    public void publishDuration(long nanos) {
        publishDuration.record(nanos / 1e9);
    }

    public void fileStartedProcessing() {
        filesInProgress.incrementAndGet();
    }

    public void fileFinishedProcessing() {
        filesInProgress.decrementAndGet();
        progressPpm.set(0);
    }

    public void progress(long offset, long size) {
        progressPpm.set(size <= 0 ? 1_000_000 : Math.min(1_000_000, offset * 1_000_000 / size));
    }

    /** A gauge whose value comes from the application, e.g. {@code sdh.inbound.oldest_file_age}. */
    public void gauge(String name, String unit, String description, LongSupplier value) {
        meter.gaugeBuilder(name).ofLongs().setUnit(unit).setDescription(description)
                .buildWithCallback(m -> m.record(value.getAsLong()));
    }
}
