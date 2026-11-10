/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import java.util.List;
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

    /**
     * Bucket boundaries, in seconds, for waits and listings: from a few
     * milliseconds to the five-minute backoff.
     */
    private static final List<Double> SECONDS_BUCKETS = List.of(0.005, 0.01,
            0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
            120.0, 300.0);

    /** Bucket boundaries for messages per batch, up to the 1,000 limit. */
    private static final List<Long> BATCH_BUCKETS = List.of(1L, 2L, 5L, 10L,
            25L, 50L, 100L, 250L, 500L, 750L, 1000L);

    /** The installed instance; one that does nothing until then. */
    private static volatile Telemetry current =
            new Telemetry(OpenTelemetry.noop(), () ->
            {
            });

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

    /** Records committed, by format. */
    private final LongCounter recordsStored;

    /** {@code record.stored} messages the broker acknowledged. */
    private final LongCounter recordsPublished;

    /** Files processed to the end. */
    private final LongCounter itemsCompleted;

    /** Items landed and announced, by intake and kind. */
    private final LongCounter itemsLanded;

    /** Bytes of the items landed and announced, by intake and kind. */
    private final LongCounter bytesLanded;

    /**
     * Messages and pushes sent again and recognised as already landed, by
     * intake.
     */
    private final LongCounter duplicatesSkipped;

    /** Messages landed per batch, by intake. */
    private final LongHistogram batchSize;

    /** How long listing a source takes, in seconds, by intake. */
    private final DoubleHistogram listDuration;

    /** HTTP pushes, by intake and result. */
    private final LongCounter pushRequests;

    /** How long a push waits for the intake, in seconds, by intake. */
    private final DoubleHistogram pushWait;

    /** Files that failed, by error code and stage. */
    private final LongCounter itemsFailed;

    /**
     * Records stored in their original form after failing conversion, by
     * error code and format.
     */
    private final LongCounter recordsFailed;

    /** How long commits take, in seconds, by table. */
    private final DoubleHistogram commitDuration;

    /** How long publishing a batch's messages takes, in seconds. */
    private final DoubleHistogram publishDuration;

    /**
     * Records received again under a different version than they were
     * stored with, by stage (STAGES.md §6).
     */
    private final LongCounter versionDiffers;

    /**
     * Records stored after a newer object at their place, by stage
     * (STAGES.md §15.8.3).
     */
    private final LongCounter supersededOnArrival;

    /**
     * Objects a reprocess processed again, by stage, cause and outcome
     * (STAGES.md §15.8.8).
     */
    private final LongCounter reprocessed;

    /**
     * Items the split stage held, begun under another of its versions
     * (STAGES.md §14.3).
     */
    private final LongCounter itemsHeld;

    /**
     * Generations the split stage stopped part way for a newer one
     * (STAGES.md §15.8.6).
     */
    private final LongCounter generationsInterrupted;

    /**
     * Failures a stage recorded in its fail store, by stage (STAGES.md
     * §15.3).
     */
    private final LongCounter failuresRecorded;

    /** Exports what is buffered, so a process about to die loses nothing. */
    private final Runnable flush;

    /** How many files are being processed now. */
    private final AtomicLong itemsInProgress = new AtomicLong();

    /**
     * How far the file being split has got, in parts per million of its
     * size; 0 when no file is being split.
     */
    private final AtomicLong progressPerMillion = new AtomicLong();

    /**
     * Creates the tracer, the event logger and the metrics.
     *
     * @param openTelemetry the OpenTelemetry instance to record with
     * @param flush         exports what is buffered
     */
    private Telemetry(OpenTelemetry openTelemetry, Runnable flush)
    {
        this.openTelemetry = openTelemetry;
        this.flush = flush;
        this.tracer = openTelemetry.getTracer(SCOPE);
        this.eventLogger = openTelemetry.getLogsBridge().get(SCOPE);
        this.meter = openTelemetry.getMeter(SCOPE);
        bytesRead = meter.counterBuilder("sdh.bytes.read").setUnit("By")
                .setDescription("Source bytes read, by stage (split, archive)")
                .build();
        recordsStored = meter.counterBuilder("sdh.records.stored")
                .setDescription("Records committed").build();
        recordsPublished = meter.counterBuilder("sdh.records.published")
                .setDescription(
                        "record.stored messages acknowledged by the broker")
                .build();
        itemsCompleted = meter.counterBuilder("sdh.items.completed").build();
        itemsLanded = meter.counterBuilder("sdh.landing.items")
                .setUnit("{item}")
                .setDescription("Items landed and announced, by intake and"
                        + " kind")
                .build();
        bytesLanded = meter.counterBuilder("sdh.landing.bytes").setUnit("By")
                .setDescription("Bytes of the items landed and announced, by"
                        + " intake and kind")
                .build();
        duplicatesSkipped = meter.counterBuilder("sdh.landing.duplicates")
                .setDescription("Messages and pushes sent again and"
                        + " recognised as already landed, by intake")
                .build();
        batchSize = meter.histogramBuilder("sdh.landing.batch_size").ofLongs()
                .setUnit("{message}")
                .setDescription("Messages landed per batch, by intake")
                .setExplicitBucketBoundariesAdvice(BATCH_BUCKETS).build();
        listDuration = meter.histogramBuilder("sdh.intake.list_duration")
                .setUnit("s")
                .setDescription("How long listing the source takes, by intake")
                .setExplicitBucketBoundariesAdvice(SECONDS_BUCKETS).build();
        pushRequests = meter.counterBuilder("sdh.push.requests")
                .setUnit("{request}")
                .setDescription("HTTP pushes, by intake and result").build();
        pushWait = meter.histogramBuilder("sdh.push.wait_duration")
                .setUnit("s")
                .setDescription("How long a push waits for the intake, by"
                        + " intake")
                .setExplicitBucketBoundariesAdvice(SECONDS_BUCKETS).build();
        itemsFailed = meter.counterBuilder("sdh.items.failed")
                .setDescription("By error code and stage").build();
        recordsFailed = meter.counterBuilder("sdh.records.failed")
                .setDescription("Records stored in their original form after"
                        + " failing conversion, by error code")
                .build();
        commitDuration = meter.histogramBuilder("sdh.commit.duration")
                .setUnit("s").build();
        publishDuration = meter.histogramBuilder("sdh.publish.duration")
                .setUnit("s").build();
        versionDiffers = meter.counterBuilder("sdh.stage.version_differs")
                .setUnit("{record}")
                .setDescription("Records received again under a different"
                        + " version than they were stored with, by stage")
                .build();
        reprocessed = meter.counterBuilder("sdh.stage.reprocessed")
                .setUnit("{record}")
                .setDescription("Objects processed again by a reprocess, by"
                        + " stage, cause and outcome")
                .build();
        supersededOnArrival = meter.counterBuilder(
                "sdh.stage.superseded_on_arrival")
                .setUnit("{record}")
                .setDescription("Records stored after a newer one at their"
                        + " place, by stage")
                .build();
        itemsHeld = meter.counterBuilder("sdh.split.items_held")
                .setUnit("{item}")
                .setDescription("Items held part way, begun under another"
                        + " version of the split stage")
                .build();
        generationsInterrupted = meter.counterBuilder(
                "sdh.split.generations_interrupted")
                .setUnit("{generation}")
                .setDescription("Generations of items stopped part way for"
                        + " a newer one")
                .build();
        failuresRecorded = meter.counterBuilder("sdh.stage.failures")
                .setUnit("{failure}")
                .setDescription("Failures recorded in a stage's fail store,"
                        + " by stage")
                .build();
        meter.gaugeBuilder("sdh.items.in_progress").ofLongs()
                .buildWithCallback(measurement ->
                        measurement.record(itemsInProgress.get()));
        meter.gaugeBuilder("sdh.item.progress_ratio")
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
        return install(openTelemetry, () ->
        {
        });
    }

    /**
     * Installs the application's OpenTelemetry instance, with a way to
     * export what it has buffered.
     *
     * @param openTelemetry the application's OpenTelemetry instance
     * @param flush         exports the buffered audit events and metrics,
     *                      waiting a short while at most; must not throw
     * @return the new telemetry
     */
    public static Telemetry install(OpenTelemetry openTelemetry,
            Runnable flush)
    {
        current = new Telemetry(openTelemetry, flush);
        return current;
    }

    /**
     * Exports the audit events and metrics buffered so far, before
     * something that may stop the process (STAGES.md §6). They are
     * otherwise exported in batches, and those of the last few seconds are
     * lost with a process that dies. The console log is written at once,
     * and needs no flush.
     */
    public void flush()
    {
        flush.run();
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
     * @param event  the event's name, such as {@code record.failed}
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
     * Counts committed records.
     *
     * @param records how many were committed
     * @param format  the format of their file
     */
    public void recordsStored(long records, String format)
    {
        recordsStored.add(records,
                Fields.of(Attr.FORMAT, format).toAttributes());
    }

    /**
     * Counts {@code record.stored} messages the broker acknowledged.
     *
     * @param messages how many were acknowledged
     */
    public void recordsPublished(long messages)
    {
        recordsPublished.add(messages);
    }

    /**
     * Counts a record stored in its original form after failing
     * conversion.
     *
     * @param code   why conversion failed
     * @param format the format of its file
     */
    public void recordFailed(ErrorCode code, String format)
    {
        recordsFailed.add(1, Fields.of(Attr.ERROR_CODE, code)
                .with(Attr.FORMAT, format).toAttributes());
    }

    /**
     * Counts an item landed and announced with {@code item.landed}, and its
     * bytes.
     *
     * @param intake the intake it came in through
     * @param kind   its kind, as on the wire
     * @param bytes  its size
     */
    public void itemLanded(String intake, String kind, long bytes)
    {
        Attributes attributes =
                Fields.of(Attr.INTAKE, intake).with(Attr.KIND, kind)
                        .toAttributes();
        itemsLanded.add(1, attributes);
        bytesLanded.add(bytes, attributes);
    }

    /**
     * Counts messages or pushes sent again and recognised as already
     * landed, so not landed again.
     *
     * @param intake the intake they came in through
     * @param count  how many
     */
    public void duplicatesSkipped(String intake, long count)
    {
        if (count > 0)
        {
            duplicatesSkipped.add(count,
                    Fields.of(Attr.INTAKE, intake).toAttributes());
        }
    }

    /**
     * Records how many messages a landed batch held.
     *
     * @param intake   the intake
     * @param messages the messages landed in it
     */
    public void batchLanded(String intake, long messages)
    {
        batchSize.record(messages,
                Fields.of(Attr.INTAKE, intake).toAttributes());
    }

    /**
     * Records how long listing the source took.
     *
     * @param intake the intake
     * @param nanos  how long, in nanoseconds
     */
    public void listed(String intake, long nanos)
    {
        listDuration.record(nanos / NANOS_PER_SECOND,
                Fields.of(Attr.INTAKE, intake).toAttributes());
    }

    /**
     * Counts an HTTP push by its result, and records how long it waited for
     * the intake.
     *
     * @param intake     the intake
     * @param result     what happened to it, such as {@code landed}
     * @param waitNanos  how long it waited for the intake, in nanoseconds
     */
    public void pushed(String intake, String result, long waitNanos)
    {
        Fields fields = Fields.of(Attr.INTAKE, intake);
        pushRequests.add(1, fields.with(Attr.RESULT, result).toAttributes());
        pushWait.record(waitNanos / NANOS_PER_SECOND, fields.toAttributes());
    }

    /** Counts a file processed to the end. */
    public void itemCompleted()
    {
        itemsCompleted.add(1);
    }

    /**
     * Counts a failed file.
     *
     * @param code  why it failed
     * @param stage the stage it failed in, such as {@code split}
     */
    public void itemFailed(ErrorCode code, String stage)
    {
        itemsFailed.add(1, Fields.of(Attr.ERROR_CODE, code)
                .with(Attr.STAGE, stage).toAttributes());
    }

    /**
     * Counts a record received again under a different version than it
     * was stored with.
     *
     * @param stage the stage
     */
    public void versionDiffers(String stage)
    {
        versionDiffers.add(1, Fields.of(Attr.STAGE, stage).toAttributes());
    }

    /**
     * Counts a record stored after a newer one at its place (STAGES.md
     * §15.8.3).
     *
     * @param stage the stage
     */
    public void supersededOnArrival(String stage)
    {
        supersededOnArrival.add(1,
                Fields.of(Attr.STAGE, stage).toAttributes());
    }

    /**
     * Counts an object a reprocess processed again (STAGES.md §15.8.8).
     *
     * @param stage   the stage
     * @param cause   why: {@code reprocess}
     * @param outcome {@code stored} or {@code failed}
     */
    public void reprocessed(String stage, String cause, String outcome)
    {
        reprocessed.add(1, Fields.of(Attr.STAGE, stage).with(Attr.CAUSE, cause)
                .with(Attr.OUTCOME, outcome).toAttributes());
    }

    /**
     * Counts a failure recorded in a stage's fail store.
     *
     * @param stage the stage
     */
    public void failureRecorded(String stage)
    {
        failuresRecorded.add(1, Fields.of(Attr.STAGE, stage).toAttributes());
    }

    /** Counts an item the split stage held (STAGES.md §14.3). */
    public void itemHeld()
    {
        itemsHeld.add(1);
    }

    /**
     * Counts a generation the split stage stopped part way for a newer one
     * (STAGES.md §15.8.6).
     */
    public void generationInterrupted()
    {
        generationsInterrupted.add(1);
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
    public void itemStartedProcessing()
    {
        itemsInProgress.incrementAndGet();
    }

    /**
     * Counts a file as no longer being processed, and resets the progress
     * gauge to 0.
     */
    public void itemFinishedProcessing()
    {
        itemsInProgress.decrementAndGet();
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

    /**
     * Adds a gauge whose value comes from the application, with
     * attributes, such as the intake it describes.
     *
     * @param name        the gauge's name
     * @param unit        its unit, such as {@code s}
     * @param description what it measures
     * @param fields      its attributes
     * @param value       gives its value each time it is read
     */
    public void gauge(String name, String unit, String description,
            Fields fields, LongSupplier value)
    {
        Attributes attributes = fields.toAttributes();
        meter.gaugeBuilder(name).ofLongs().setUnit(unit)
                .setDescription(description)
                .buildWithCallback(measurement ->
                        measurement.record(value.getAsLong(), attributes));
    }
}
