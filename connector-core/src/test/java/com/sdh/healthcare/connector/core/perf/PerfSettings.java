/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.perf;

import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * The settings every performance test reads (design §13.1), from system
 * properties the {@code perf} profile passes on. The same names mean the
 * same thing in every stage, so the stages' results can be compared.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PerfSettings
{

    /** The core benchmark: split, encrypt and store, without the apps. */
    public static final String CORE = "core";

    /** The landing node. */
    public static final String LANDING = "landing";

    /** The splitter. */
    public static final String SPLIT = "split";

    /** Landing and splitting together, from the timelines. */
    public static final String END_TO_END = "end-to-end";

    /** One large C-CDA file. */
    public static final String BULK_CCDA = "bulk-ccda";

    /** One large X12 file. */
    public static final String BULK_X12 = "bulk-x12";

    /** Many small C-CDA documents, arriving at a steady rate. */
    public static final String SMALL_CCDA = "small-ccda";

    /** A directory intake. */
    public static final String DIRECTORY = "directory";

    /** An HTTP push intake. */
    public static final String HTTP = "http";

    /** Not instantiated: this class only has static methods. */
    private PerfSettings()
    {
    }

    /**
     * Whether a stage is to run ({@code perf.stages}, default all three).
     *
     * @param stage {@link #CORE}, {@link #LANDING} or {@link #SPLIT}
     * @return true if it is selected
     */
    public static boolean runs(String stage)
    {
        return list("perf.stages", "core,landing,split").contains(stage);
    }

    /**
     * The workloads to run ({@code perf.workloads}).
     *
     * @return {@link #BULK_CCDA}, {@link #BULK_X12} or {@link #SMALL_CCDA},
     *         in order
     */
    public static List<String> workloads()
    {
        return list("perf.workloads",
                BULK_CCDA + "," + BULK_X12 + "," + SMALL_CCDA);
    }

    /**
     * The landing intakes the small workload goes through
     * ({@code perf.intakes}); bulk files always go through a directory.
     *
     * @return {@link #DIRECTORY} or {@link #HTTP}, in order
     */
    public static List<String> intakes()
    {
        return list("perf.intakes", DIRECTORY + "," + HTTP);
    }

    /**
     * The size of each bulk file ({@code perf.size}, default 1 GiB).
     *
     * @return bytes
     */
    public static long size()
    {
        return parseSize(System.getProperty("perf.size", "1GiB"));
    }

    /**
     * How many small documents ({@code perf.items}, default 1,000).
     *
     * @return the count
     */
    public static int items()
    {
        return Integer.getInteger("perf.items", 1000);
    }

    /**
     * How many small documents arrive each second ({@code perf.rate},
     * default 10); 0 puts them all there at once.
     *
     * @return items per second
     */
    public static double rate()
    {
        return Double.parseDouble(System.getProperty("perf.rate", "10"));
    }

    /**
     * Where the tables are ({@code perf.storage}).
     *
     * @return {@code local} (default) or {@code s3}
     */
    public static String storage()
    {
        return System.getProperty("perf.storage", "local");
    }

    /**
     * Where every stage writes its results ({@code perf.out}): the root
     * project's {@code target/perf}, so the stages share one history.
     *
     * @return the directory
     */
    public static Path out()
    {
        return Path.of(System.getProperty("perf.out", "target/perf"))
                .toAbsolutePath().normalize();
    }

    /**
     * Parses a size such as {@code 1GiB} or {@code 512MB}.
     *
     * @param text the size; binary units either way
     * @return bytes
     */
    public static long parseSize(String text)
    {
        String value = text.strip().toUpperCase(Locale.ROOT);
        long multiplier = 1;
        for (String[] unit : new String[][] {{"GIB", "1073741824"},
            {"GB", "1073741824"}, {"MIB", "1048576"}, {"MB", "1048576"},
            {"KIB", "1024"}, {"KB", "1024"}})
        {
            if (value.endsWith(unit[0]))
            {
                multiplier = Long.parseLong(unit[1]);
                value = value.substring(0, value.length() - unit[0].length())
                        .strip();
                break;
            }
        }
        return (long) (Double.parseDouble(value) * multiplier);
    }

    /**
     * A comma-separated list setting.
     *
     * @param property     the system property
     * @param defaultValue its value when unset
     * @return the entries, trimmed, without empty ones
     */
    private static List<String> list(String property, String defaultValue)
    {
        return Stream.of(System.getProperty(property, defaultValue)
                .split(",")).map(String::strip)
                .filter(entry -> !entry.isEmpty()).toList();
    }
}
