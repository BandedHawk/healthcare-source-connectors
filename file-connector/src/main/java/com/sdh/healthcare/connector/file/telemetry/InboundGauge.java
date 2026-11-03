/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.telemetry;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.stream.Stream;

/**
 * The {@code sdh.inbound.oldest_file_age} gauge (design §12): how long the
 * oldest file has been waiting in the inbound directory.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class InboundGauge
{

    /** How many milliseconds there are in a second. */
    private static final long MILLIS_PER_SECOND = 1000;

    /** Not used: there are only static methods. */
    private InboundGauge()
    {
    }

    /**
     * Adds the gauge.
     *
     * @param telemetry the installed telemetry
     * @param inbound   the inbound directory; null for none
     */
    public static void register(Telemetry telemetry, Path inbound)
    {
        telemetry.gauge("sdh.inbound.oldest_file_age", "s",
                "Age of the oldest file waiting in the inbound directory",
                () -> oldestAgeSeconds(inbound));
    }

    /**
     * How long ago the oldest waiting file was last changed. Hidden files
     * and {@code .done} markers don't count.
     *
     * @param directory the inbound directory; null for none
     * @return the age in seconds; 0 if no file is waiting or the directory
     *         can't be read
     */
    static long oldestAgeSeconds(Path directory)
    {
        if (directory == null)
        {
            return 0;
        }
        try (Stream<Path> files = Files.list(directory))
        {
            long oldest = files.filter(Files::isRegularFile)
                    .filter(file -> !file.getFileName().toString()
                            .startsWith(".")
                            && !file.getFileName().toString()
                                    .endsWith(".done"))
                    .mapToLong(InboundGauge::modifiedMillis)
                    .min().orElse(Long.MAX_VALUE);
            return oldest == Long.MAX_VALUE ? 0 : Math.max(0,
                    (Instant.now().toEpochMilli() - oldest)
                            / MILLIS_PER_SECOND);
        }
        catch (IOException unreadable)
        {
            return 0;
        }
    }

    /**
     * When a file was last changed.
     *
     * @param file the file
     * @return the time in milliseconds; the largest long if it can't be
     *         read, so it never counts as the oldest
     */
    private static long modifiedMillis(Path file)
    {
        try
        {
            return Files.getLastModifiedTime(file).toMillis();
        }
        catch (IOException unreadable)
        {
            return Long.MAX_VALUE;
        }
    }
}
