/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.registry.otlp.OtlpConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The JVM's metrics are exported, and nothing else is (design §12.2).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class JvmMetricsExportTest
{

    /** The service the resource names. */
    private static final Resource RESOURCE = Resource.create(Attributes.of(
            AttributeKey.stringKey("service.name"), "test-stage"));

    /**
     * Settings for a test.
     *
     * @param endpoint   the collector, or null
     * @param jvmMetrics whether the JVM's metrics are on
     * @return the settings
     */
    private static CommonSettings.Telemetry telemetry(String endpoint,
            boolean jvmMetrics)
    {
        return new CommonSettings.Telemetry(new CommonSettings.Otlp(endpoint,
                Map.of("Authorization", "Bearer test"), Duration.ofSeconds(10)),
                Duration.ofSeconds(30), jvmMetrics);
    }

    /**
     * Meters of every kind registered through the filter: the JVM's
     * binders, and others that must not leave.
     *
     * @return the registry, filtered as the stages' OTLP registry is
     */
    private static MeterRegistry filtered()
    {
        OtlpMeterRegistry registry = OtlpMeterRegistry.builder(
                JvmMetricsExport.config(telemetry(null, true), RESOURCE))
                .build();
        new TelemetryConfiguration().sdhJvmMetricsOnly().customize(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        registry.counter("http.server.requests", "uri", "/intakes/x/items")
                .increment();
        registry.counter("kafka.consumer.records", "topic", "x.records")
                .increment();
        registry.gauge("jvm.extra", List.of(Tag.of("area", "heap"),
                Tag.of("item_id", "a-value")), 1.0);
        return registry;
    }

    /** The JVM's meters pass, with their pools, collectors and states. */
    @Test
    void theJvmsMetersPass()
    {
        MeterRegistry registry = filtered();

        assertThat(registry.find("jvm.memory.used").meters()).isNotEmpty()
                .allSatisfy(meter -> assertThat(meter.getId().getTag("area"))
                        .isNotNull());
        assertThat(registry.find("jvm.memory.used").tag("area", "heap")
                .meters()).isNotEmpty();
        assertThat(registry.find("jvm.threads.states").meters())
                .anySatisfy(meter -> assertThat(meter.getId().getTag("state"))
                        .isNotNull());
        assertThat(registry.find("system.cpu.count").meters()).isNotEmpty();
        assertThat(registry.find("jvm.gc.max.data.size").meters())
                .isNotEmpty();
    }

    /**
     * Nothing else passes: no meter named otherwise, and no tag the JVM
     * doesn't name, even on a meter named for the JVM.
     */
    @Test
    void nothingElsePasses()
    {
        MeterRegistry registry = filtered();

        assertThat(registry.find("http.server.requests").meters()).isEmpty();
        assertThat(registry.find("kafka.consumer.records").meters())
                .isEmpty();
        assertThat(registry.find("jvm.extra").meters()).singleElement()
                .satisfies(meter -> assertThat(meter.getId().getTags())
                        .containsExactly(Tag.of("area", "heap")));
        for (Meter meter : registry.getMeters())
        {
            assertThat(JvmMetricsExport.allowed(meter.getId().getName()))
                    .as(meter.getId().getName()).isTrue();
            assertThat(meter.getId().getTags())
                    .allSatisfy(tag -> assertThat(JvmMetricsExport.TAGS)
                            .contains(tag.getKey()));
        }
    }

    /**
     * The registry sends to sdh.telemetry's collector, with its headers,
     * interval and resource.
     */
    @Test
    void theRegistryUsesTheTelemetrySettings()
    {
        OtlpConfig config = JvmMetricsExport.config(
                telemetry("http://collector:4318/", true), RESOURCE);

        assertThat(config.enabled()).isTrue();
        assertThat(config.url()).isEqualTo("http://collector:4318/v1/metrics");
        assertThat(config.headers())
                .containsEntry("Authorization", "Bearer test");
        assertThat(config.step()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.resourceAttributes())
                .containsEntry("service.name", "test-stage");
    }

    /** Without an endpoint, or with the switch off, nothing is sent. */
    @Test
    void nothingIsSentWithoutAnEndpointOrWithTheSwitchOff()
    {
        assertThat(JvmMetricsExport.config(telemetry(null, true), RESOURCE)
                .enabled()).isFalse();
        assertThat(JvmMetricsExport.config(telemetry(" ", true), RESOURCE)
                .enabled()).isFalse();
        assertThat(JvmMetricsExport.config(
                telemetry("http://collector:4318", false), RESOURCE)
                .enabled()).isFalse();
    }
}
