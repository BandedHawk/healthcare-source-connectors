/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ProcessMetrics}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ProcessMetricsTest
{

    /** Stands in for the container's cgroup. */
    @TempDir
    Path cgroup;

    /**
     * Reads the counters' values, by name.
     *
     * @param reader the reader
     * @return each counter's value, in seconds
     */
    static Map<String, Double> values(InMemoryMetricReader reader)
    {
        Map<String, Double> values = new HashMap<>();
        for (MetricData metric : reader.collectAllMetrics())
        {
            metric.getDoubleSumData().getPoints().forEach(
                    point -> values.put(metric.getName(), point.getValue()));
        }
        return values;
    }

    /**
     * The CPU time and the time waited for a CPU come from the cgroup's
     * files, in seconds; GC time from the JVM.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void readsTheCgroupAndTheJvm() throws Exception
    {
        Files.writeString(cgroup.resolve("cpu.stat"),
                "usage_usec 2500000\nuser_usec 2000000\n"
                        + "system_usec 500000\nnr_throttled 0\n");
        Files.writeString(cgroup.resolve("cpu.pressure"),
                "some avg10=1.00 avg60=0.50 avg300=0.10 total=750000\n"
                        + "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n");
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        try (SdkMeterProvider provider = SdkMeterProvider.builder()
                .registerMetricReader(reader).build())
        {
            ProcessMetrics.register(provider.get("test"), cgroup);
            Map<String, Double> values = values(reader);
            assertThat(values).containsEntry("sdh.process.cpu_time", 2.5)
                    .containsEntry("sdh.process.cpu_wait", 0.75)
                    .containsKey("sdh.process.gc_time");
        }
    }

    /**
     * Without a cgroup, as on a laptop, only GC time is reported.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void reportsOnlyGcWithoutACgroup() throws Exception
    {
        InMemoryMetricReader reader = InMemoryMetricReader.create();
        try (SdkMeterProvider provider = SdkMeterProvider.builder()
                .registerMetricReader(reader).build())
        {
            ProcessMetrics.register(provider.get("test"),
                    cgroup.resolve("none"));
            assertThat(values(reader).keySet())
                    .containsExactly("sdh.process.gc_time");
        }
    }
}
