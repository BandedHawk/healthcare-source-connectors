/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.telemetry;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import io.micrometer.registry.otlp.OtlpConfig;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The JVM's metrics, as Micrometer's binders register them, exported by the
 * Micrometer OTLP registry (design §12.2). Only meters named for the JVM,
 * the process or the system pass, and only with tags the JVM names: no
 * other meter in the registry, and no other tag, leaves the process.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class JvmMetricsExport
{

    /** The start of every name exported. */
    static final List<String> NAME_PREFIXES =
            List.of("jvm.", "process.", "system.");

    /**
     * The tags kept: heap or non-heap, a memory or buffer pool, a
     * collector, a collection's action and cause, a thread state, and the
     * JVM's version, vendor and runtime.
     */
    static final Set<String> TAGS = Set.of("area", "id", "gc", "action",
            "cause", "state", "version", "vendor", "runtime");

    /** Not made: static methods only. */
    private JvmMetricsExport()
    {
    }

    /**
     * Whether a meter's name is exported.
     *
     * @param name the meter's name
     * @return true if it starts with one of {@link #NAME_PREFIXES}
     */
    static boolean allowed(String name)
    {
        for (String prefix : NAME_PREFIXES)
        {
            if (name.startsWith(prefix))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * The filter for the OTLP registry: denies every meter whose name isn't
     * allowed, and removes every tag not in {@link #TAGS} from those that
     * are.
     *
     * @return the filter
     */
    public static MeterFilter filter()
    {
        return new MeterFilter()
        {
            @Override
            public MeterFilterReply accept(Meter.Id id)
            {
                return allowed(id.getName())
                        ? MeterFilterReply.NEUTRAL : MeterFilterReply.DENY;
            }

            @Override
            public Meter.Id map(Meter.Id id)
            {
                List<Tag> kept = id.getTags().stream()
                        .filter(tag -> TAGS.contains(tag.getKey()))
                        .toList();
                return kept.size() == id.getTags().size()
                        ? id : id.replaceTags(kept);
            }
        };
    }

    /**
     * The OTLP registry's settings, from {@code sdh.telemetry} rather than
     * {@code management.otlp.metrics.export}: the same collector, headers
     * and interval as the stage's own metrics, and the same resource, so
     * both name the same service and pod. Publishes only with an endpoint
     * and {@code jvm-metrics} on.
     *
     * @param telemetry the export settings
     * @param resource  the OpenTelemetry SDK's resource
     * @return the settings
     */
    public static OtlpConfig config(CommonSettings.Telemetry telemetry,
            Resource resource)
    {
        CommonSettings.Otlp otlp = telemetry.otlp();
        boolean enabled = telemetry.jvmMetrics() && otlp.endpoint() != null
                && !otlp.endpoint().isBlank();
        Map<String, String> attributes = new LinkedHashMap<>();
        resource.getAttributes().forEach((key, value) ->
                attributes.put(key.getKey(), String.valueOf(value)));
        Map<String, String> headers = TelemetryConfiguration.headers(otlp);
        Duration step = telemetry.metricInterval();
        return new OtlpConfig()
        {
            @Override
            public String get(String key)
            {
                return null;
            }

            @Override
            public boolean enabled()
            {
                return enabled;
            }

            @Override
            public String url()
            {
                return enabled
                        ? TelemetryConfiguration.url(otlp, "/v1/metrics")
                        : OtlpConfig.super.url();
            }

            @Override
            public Map<String, String> headers()
            {
                return headers;
            }

            @Override
            public Duration step()
            {
                return step;
            }

            @Override
            public Map<String, String> resourceAttributes()
            {
                return attributes;
            }
        };
    }
}
