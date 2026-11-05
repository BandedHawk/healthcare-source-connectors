/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.data.LogRecordData;
import io.opentelemetry.sdk.logs.export.SimpleLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryLogRecordExporter;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Installs an OpenTelemetry SDK that keeps what is recorded in memory, for
 * one test at a time, so a test can check the metrics and audit events the
 * code under test produced. Register it as a field with
 * {@code @RegisterExtension}; the no-op telemetry is put back after each
 * test.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TelemetryCapture
        implements BeforeEachCallback, AfterEachCallback
{

    /** Reads the metrics on demand. */
    private InMemoryMetricReader metrics;

    /** Keeps the audit events. */
    private InMemoryLogRecordExporter events;

    /** The SDK installed for the current test. */
    private OpenTelemetrySdk sdk;

    /**
     * Creates the extension; nothing is installed until a test starts.
     */
    public TelemetryCapture()
    {
    }

    /**
     * Installs a fresh SDK.
     *
     * @param context the test's context
     */
    @Override
    public void beforeEach(ExtensionContext context)
    {
        metrics = InMemoryMetricReader.create();
        events = InMemoryLogRecordExporter.create();
        sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(SdkMeterProvider.builder()
                        .registerMetricReader(metrics).build())
                .setLoggerProvider(SdkLoggerProvider.builder()
                        .addLogRecordProcessor(
                                SimpleLogRecordProcessor.create(events))
                        .build())
                .build();
        Telemetry.install(sdk);
    }

    /**
     * Puts the no-op telemetry back and closes the SDK.
     *
     * @param context the test's context
     */
    @Override
    public void afterEach(ExtensionContext context)
    {
        Telemetry.install(OpenTelemetry.noop());
        sdk.close();
    }

    /**
     * The sum of a counter's points, or a gauge's, whose attributes
     * include the ones given.
     *
     * @param name       the metric's name
     * @param attributes the attributes a point must have
     * @return the sum; 0 if none match
     */
    public long value(String name, Attributes attributes)
    {
        return metric(name).flatMap(metric -> Stream.concat(
                        metric.getLongSumData().getPoints().stream(),
                        metric.getLongGaugeData().getPoints().stream()))
                .filter(point -> matches(point, attributes))
                .mapToLong(LongPointData::getValue).sum();
    }

    /**
     * How many values a histogram has recorded, over all its points whose
     * attributes include the ones given.
     *
     * @param name       the histogram's name
     * @param attributes the attributes a point must have
     * @return the count; 0 if none match
     */
    public long count(String name, Attributes attributes)
    {
        return histogramPoints(name, attributes)
                .mapToLong(HistogramPointData::getCount).sum();
    }

    /**
     * The sum of the values a histogram has recorded, over all its points
     * whose attributes include the ones given.
     *
     * @param name       the histogram's name
     * @param attributes the attributes a point must have
     * @return the sum; 0 if none match
     */
    public double sum(String name, Attributes attributes)
    {
        return histogramPoints(name, attributes)
                .mapToDouble(HistogramPointData::getSum).sum();
    }

    /**
     * Whether a metric has been recorded at all.
     *
     * @param name the metric's name
     * @return true if it has
     */
    public boolean has(String name)
    {
        return metric(name).findAny().isPresent();
    }

    /**
     * The audit events emitted so far, by name, in order.
     *
     * @return the names, such as {@code intake.unreachable}
     */
    public List<String> events()
    {
        return events.getFinishedLogRecordItems().stream()
                .map(LogRecordData::getEventName)
                .map(name -> name.substring("sdh.audit.".length()))
                .toList();
    }

    /**
     * The attributes of the audit events of one name, in order.
     *
     * @param event the event's name, such as {@code intake.unreachable}
     * @return their attributes
     */
    public List<Attributes> attributesOf(String event)
    {
        return events.getFinishedLogRecordItems().stream()
                .filter(record -> ("sdh.audit." + event)
                        .equals(record.getEventName()))
                .map(LogRecordData::getAttributes)
                .toList();
    }

    /**
     * The metric of a name, as last collected.
     *
     * @param name the metric's name
     * @return it, or nothing if it hasn't been recorded
     */
    private Stream<MetricData> metric(String name)
    {
        Collection<MetricData> collected = metrics.collectAllMetrics();
        return collected.stream()
                .filter(metric -> metric.getName().equals(name));
    }

    /**
     * A histogram's points whose attributes include the ones given.
     *
     * @param name       the histogram's name
     * @param attributes the attributes a point must have
     * @return the points
     */
    private Stream<HistogramPointData> histogramPoints(String name,
            Attributes attributes)
    {
        return metric(name).flatMap(metric -> metric.getHistogramData()
                        .getPoints().stream())
                .filter(point -> matches(point, attributes));
    }

    /**
     * Whether a point's attributes include the ones given.
     *
     * @param point      the point
     * @param attributes the attributes it must have
     * @return true if it has them all
     */
    private static boolean matches(PointData point, Attributes attributes)
    {
        Map<?, Object> have = point.getAttributes().asMap();
        return attributes.asMap().entrySet().stream().allMatch(entry ->
                entry.getValue().equals(have.get(entry.getKey())));
    }
}
