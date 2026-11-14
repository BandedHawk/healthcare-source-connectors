/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import io.opentelemetry.api.metrics.Meter;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

/**
 * What the process gets of the machine, so a stage that slows under load
 * can be told short of CPU from waiting on storage (docs/PERFORMANCE.md
 * §10): the CPU time its container used, the time its threads waited for
 * a CPU they were ready to run on, and the time the JVM spent collecting
 * garbage. The first two come from the container's cgroup (v2), and are
 * left out where there is none, as on a laptop; the pod reports them
 * itself, so they outlive pods scaled away, and need no access to the
 * nodes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ProcessMetrics
{

    /** The container's cgroup. */
    private static final Path CGROUP = Path.of("/sys/fs/cgroup");

    /** Not instantiated. */
    private ProcessMetrics()
    {
    }

    /**
     * Adds the counters, in seconds: {@code sdh.process.cpu_time},
     * {@code sdh.process.cpu_wait} and {@code sdh.process.gc_time}.
     *
     * @param meter the meter they are added to
     */
    static void register(Meter meter)
    {
        register(meter, CGROUP);
    }

    /**
     * Adds the counters, from a cgroup's files.
     *
     * @param meter  the meter they are added to
     * @param cgroup the cgroup's directory
     */
    static void register(Meter meter, Path cgroup)
    {
        Path stat = cgroup.resolve("cpu.stat");
        Path pressure = cgroup.resolve("cpu.pressure");
        if (Files.isReadable(stat))
        {
            meter.counterBuilder("sdh.process.cpu_time").ofDoubles()
                    .setUnit("s")
                    .setDescription("CPU time the container used")
                    .buildWithCallback(measurement -> cpuTime(stat)
                            .ifPresent(micros -> measurement.record(
                                    micros / 1e6)));
        }
        if (Files.isReadable(pressure))
        {
            meter.counterBuilder("sdh.process.cpu_wait").ofDoubles()
                    .setUnit("s")
                    .setDescription("Time some of the container's threads"
                            + " waited for a CPU")
                    .buildWithCallback(measurement -> cpuWait(pressure)
                            .ifPresent(micros -> measurement.record(
                                    micros / 1e6)));
        }
        meter.counterBuilder("sdh.process.gc_time").ofDoubles()
                .setUnit("s")
                .setDescription("Time the JVM spent collecting garbage")
                .buildWithCallback(measurement -> measurement.record(
                        gcMillis() / 1e3));
    }

    /**
     * The CPU time used, from {@code cpu.stat}'s {@code usage_usec}.
     *
     * @param stat the file
     * @return microseconds; empty if it can't be read
     */
    static OptionalLong cpuTime(Path stat)
    {
        try
        {
            for (String line : Files.readAllLines(stat))
            {
                if (line.startsWith("usage_usec "))
                {
                    return OptionalLong.of(Long.parseLong(
                            line.substring("usage_usec ".length()).trim()));
                }
            }
        }
        catch (IOException | NumberFormatException unreadable)
        {
            // Not reported this time.
        }
        return OptionalLong.empty();
    }

    /**
     * The time waited for a CPU, from {@code cpu.pressure}'s
     * {@code some ... total=}.
     *
     * @param pressure the file
     * @return microseconds; empty if it can't be read
     */
    static OptionalLong cpuWait(Path pressure)
    {
        try
        {
            for (String line : Files.readAllLines(pressure))
            {
                if (line.startsWith("some "))
                {
                    int total = line.indexOf("total=");
                    if (total >= 0)
                    {
                        return OptionalLong.of(Long.parseLong(line
                                .substring(total + "total=".length())
                                .trim()));
                    }
                }
            }
        }
        catch (IOException | NumberFormatException unreadable)
        {
            // Not reported this time.
        }
        return OptionalLong.empty();
    }

    /**
     * The time the JVM's collectors have taken.
     *
     * @return milliseconds, over all collectors
     */
    static long gcMillis()
    {
        long millis = 0;
        for (GarbageCollectorMXBean collector
                : ManagementFactory.getGarbageCollectorMXBeans())
        {
            millis += Math.max(0, collector.getCollectionTime());
        }
        return millis;
    }
}
