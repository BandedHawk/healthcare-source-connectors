/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
 * The connector's traces, metrics and audit events (design §12), on the
 * OpenTelemetry API. The application installs an OpenTelemetry SDK with
 * {@link #install}; until then, as in tests and libraries, everything does
 * nothing.
 * <p>
 * Every attribute goes through {@link Fields}, so only allowed keys exist.
 * Never record exception messages or stack traces on spans, as they could
 * hold PHI: use {@link #fail} with an error code.
 * <p>
 * Audit events are OpenTelemetry log records named
 * {@code sdh.audit.<event>}, and are also written to the console log.
 * Thread-safe.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Telemetry
{

    /** The console log that audit events are written to. */
    private static final Logger AUDIT_LOG =
            LoggerFactory.getLogger("sdh.audit");

    /** The name the connector's tracer, meter and event logger go by. */
    private static final String SCOPE = "com.sdh.healthcare.connector";

    /** A progress of 1: the whole file, in parts per million. */
    private static final long WHOLE_FILE = 1_000_000;

    /** How many nanoseconds there are in a second. */
    private static final double NANOS_PER_SECOND = 1e9;

    /** The installed instance; one that does nothing until then. */
    private static volatile Telemetry current =
            new Telemetry(OpenTelemetry.noop());

    /** The OpenTelemetry instance everything is recorded with. */
    private final OpenTelemetry openTelemetry;

    /** Starts spans. */
    private final Tracer tracer;

    /** Emits audit events as OpenTelemetry log records. */
    private final io.opentelemetry.api.logs.Logger eventLogger;

    /** Makes the metrics, and the gauges the application adds. */
    private final Meter meter;

    /** Source bytes read, by stage. */
    private final LongCounter bytesRead;

    /** Sections committed, by format. */
    private final LongCounter sectionsStored;

    /** {@code section.stored} messages the broker acknowledged. */
    private final LongCounter sectionsPublished;

    /** Files processed to the end. */
    private final LongCounter filesCompleted;

    /** Files that failed, by error code and stage. */
    private final LongCounter filesFailed;

    /**
     * Sections stored in their original form after failing conversion, by
     * error code and format.
     */
    private final LongCounter sectionsFailed;

    /** How long commits take, in seconds, by table. */
    private final DoubleHistogram commitDuration;

    /** How long publishing a batch's messages takes, in seconds. */
    private final DoubleHistogram publishDuration;

    /** How many files are being processed now. */
    private final AtomicLong filesInProgress = new AtomicLong();

    /**
     * How far the file being split has got, in parts per million of its
     * size; 0 when no file is being split.
     */
    private final AtomicLong progressPerMillion = new AtomicLong();

    /**
     * Creates the tracer, the event logger and the metrics.
     *
     * @param openTelemetry the OpenTelemetry instance to record with
     */
    private Telemetry(OpenTelemetry openTelemetry)
    {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer(SCOPE);
        this.eventLogger = openTelemetry.getLogsBridge().get(SCOPE);
        this.meter = openTelemetry.getMeter(SCOPE);
        bytesRead = meter.counterBuilder("sdh.bytes.read").setUnit("By")
                .setDescription("Source bytes read, by stage (split, archive)")
                .build();
        sectionsStored = meter.counterBuilder("sdh.sections.stored")
                .setDescription("Sections committed").build();
        sectionsPublished = meter.counterBuilder("sdh.sections.published")
                .setDescription(
                        "section.stored messages acknowledged by the broker")
                .build();
        filesCompleted = meter.counterBuilder("sdh.files.completed").build();
        filesFailed = meter.counterBuilder("sdh.files.failed")
                .setDescription("By error code and stage").build();
        sectionsFailed = meter.counterBuilder("sdh.sections.failed")
                .setDescription("Sections stored in their original form after"
                        + " failing conversion, by error code")
                .build();
        commitDuration = meter.histogramBuilder("sdh.commit.duration")
                .setUnit("s").build();
        publishDuration = meter.histogramBuilder("sdh.publish.duration")
                .setUnit("s").build();
        meter.gaugeBuilder("sdh.files.in_progress").ofLongs()
                .buildWithCallback(measurement ->
                        measurement.record(filesInProgress.get()));
        meter.gaugeBuilder("sdh.file.progress_ratio")
                .setDescription("Bytes split / file size of the current file")
                .buildWithCallback(measurement -> measurement.record(
                        progressPerMillion.get() / (double) WHOLE_FILE));
    }

    /**
     * Installs the application's OpenTelemetry instance. Components look it
     * up with {@link #get()} each time they record something, so they pick
     * it up from then on.
     *
     * @param openTelemetry the application's OpenTelemetry instance
     * @return the new telemetry
     */
    public static Telemetry install(OpenTelemetry openTelemetry)
    {
        current = new Telemetry(openTelemetry);
        return current;
    }

    /**
     * The installed telemetry.
     *
     * @return the telemetry; one that does nothing if none is installed
     */
    public static Telemetry get()
    {
        return current;
    }

    /**
     * The OpenTelemetry instance everything is recorded with.
     *
     * @return the instance
     */
    public OpenTelemetry openTelemetry()
    {
        return openTelemetry;
    }

    // ---- traces ----

    /**
     * Starts a span, a child of the current context's span. Make it current
     * with {@code makeCurrent()}, and end it when done.
     *
     * @param name   the span's name
     * @param fields its attributes
     * @return the started span
     */
    public Span span(String name, Fields fields)
    {
        return tracer.spanBuilder(name)
                .setAllAttributes(fields.toAttributes()).startSpan();
    }

    /**
     * Marks a span failed, with an error code: no message or stack trace,
     * which could hold PHI.
     *
     * @param span the span
     * @param code why it failed; null for no code
     */
    public static void fail(Span span, ErrorCode code)
    {
        if (code != null)
        {
            span.setAttribute(Attr.ERROR_CODE.key(), code.name());
        }
        span.setStatus(StatusCode.ERROR);
    }

    /**
     * Marks a span failed, with the class of the exception that failed it:
     * not its message or stack trace, which could hold PHI.
     *
     * @param span  the span
     * @param error what failed it
     */
    public static void fail(Span span, Throwable error)
    {
        span.setAttribute(Attr.REASON.key(), error.getClass().getSimpleName());
        span.setStatus(StatusCode.ERROR);
    }

    // ---- audit events ----

    /**
     * Records an audit event of something that happened as expected.
     *
     * @param event  the event's name, such as {@code file.split}
     * @param fields its attributes
     */
    public void audit(String event, Fields fields)
    {
        audit(event, Severity.INFO, fields);
    }

    /**
     * Records an audit event as a warning: something went wrong, but
     * processing carries on.
     *
     * @param event  the event's name, such as {@code section.failed}
     * @param fields its attributes
     */
    public void auditWarn(String event, Fields fields)
    {
        audit(event, Severity.WARN, fields);
    }

    /**
     * Records an audit event as an error: a file, or the connector, failed.
     *
     * @param event  the event's name, such as {@code file.failed}
     * @param fields its attributes
     */
    public void auditError(String event, Fields fields)
    {
        audit(event, Severity.ERROR, fields);
    }

    /**
     * Records an audit event as an OpenTelemetry log record, and writes it
     * to the console log.
     *
     * @param event    the event's name
     * @param severity how serious it is
     * @param fields   its attributes
     */
    private void audit(String event, Severity severity, Fields fields)
    {
        eventLogger.logRecordBuilder()
                .setEventName("sdh.audit." + event)
                .setSeverity(severity)
                .setBody(event)
                .setAllAttributes(fields.toAttributes())
                .emit();
        String line = event + fields.toLogString();
        switch (severity)
        {
            case ERROR -> AUDIT_LOG.error(line);
            case WARN -> AUDIT_LOG.warn(line);
            default -> AUDIT_LOG.info(line);
        }
    }

    // ---- metrics ----

    /**
     * Counts source bytes read.
     *
     * @param bytes how many were read
     * @param stage the stage that read them: {@code split} or {@code archive}
     */
    public void bytesRead(long bytes, String stage)
    {
        bytesRead.add(bytes, Fields.of(Attr.STAGE, stage).toAttributes());
    }

    /**
     * Counts committed sections.
     *
     * @param sections how many were committed
     * @param format   the format of their file
     */
    public void sectionsStored(long sections, String format)
    {
        sectionsStored.add(sections,
                Fields.of(Attr.FORMAT, format).toAttributes());
    }

    /**
     * Counts {@code section.stored} messages the broker acknowledged.
     *
     * @param messages how many were acknowledged
     */
    public void sectionsPublished(long messages)
    {
        sectionsPublished.add(messages);
    }

    /**
     * Counts a section stored in its original form after failing
     * conversion.
     *
     * @param code   why conversion failed
     * @param format the format of its file
     */
    public void sectionFailed(ErrorCode code, String format)
    {
        sectionsFailed.add(1, Fields.of(Attr.ERROR_CODE, code)
                .with(Attr.FORMAT, format).toAttributes());
    }

    /** Counts a file processed to the end. */
    public void fileCompleted()
    {
        filesCompleted.add(1);
    }

    /**
     * Counts a failed file.
     *
     * @param code  why it failed
     * @param stage the stage it failed in, such as {@code split}
     */
    public void fileFailed(ErrorCode code, String stage)
    {
        filesFailed.add(1, Fields.of(Attr.ERROR_CODE, code)
                .with(Attr.STAGE, stage).toAttributes());
    }

    /**
     * Records how long a commit took.
     *
     * @param nanos how long, in nanoseconds
     * @param table the table committed to
     */
    public void commitDuration(long nanos, String table)
    {
        commitDuration.record(nanos / NANOS_PER_SECOND,
                Fields.of(Attr.TABLE, table).toAttributes());
    }

    /**
     * Records how long publishing a batch's messages took.
     *
     * @param nanos how long, in nanoseconds
     */
    public void publishDuration(long nanos)
    {
        publishDuration.record(nanos / NANOS_PER_SECOND);
    }

    /** Counts a file as being processed, until it is finished. */
    public void fileStartedProcessing()
    {
        filesInProgress.incrementAndGet();
    }

    /**
     * Counts a file as no longer being processed, and resets the progress
     * gauge to 0.
     */
    public void fileFinishedProcessing()
    {
        filesInProgress.decrementAndGet();
        progressPerMillion.set(0);
    }

    /**
     * Records how far the file being split has got.
     *
     * @param offset how many of its bytes have been split
     * @param size   the file's size in bytes; 0 or less counts as done
     */
    public void progress(long offset, long size)
    {
        progressPerMillion.set(size <= 0 ? WHOLE_FILE
                : Math.min(WHOLE_FILE, offset * WHOLE_FILE / size));
    }

    /**
     * Adds a gauge whose value comes from the application, such as
     * {@code sdh.inbound.oldest_file_age}.
     *
     * @param name        the gauge's name
     * @param unit        its unit, such as {@code s}
     * @param description what it measures
     * @param value       gives its value each time it is read
     */
    public void gauge(String name, String unit, String description,
            LongSupplier value)
    {
        meter.gaugeBuilder(name).ofLongs().setUnit(unit)
                .setDescription(description)
                .buildWithCallback(measurement ->
                        measurement.record(value.getAsLong()));
    }
}
