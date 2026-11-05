/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.perf;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * When each item of a landing run became available and when it was
 * announced, relative to the run's start (design §13.1). The landing test
 * writes it; the splitter test replays the announcements at the same
 * offsets, so that end to end is each item's landing latency plus its
 * splitting latency, queueing included.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Timeline
{

    /**
     * One item.
     *
     * @param seq       its place in the workload, from 0
     * @param size      its size in bytes
     * @param available when it became available to the intake, in
     *                  nanoseconds from the run's start
     * @param announced when its {@code item.landed} message was published,
     *                  in nanoseconds from the run's start
     */
    public record Entry(int seq, long size, long available, long announced)
    {

        /**
         * Its landing latency.
         *
         * @return nanoseconds
         */
        public long landing()
        {
            return announced - available;
        }
    }

    /** The CSV header. */
    private static final String HEADER =
            "seq,size,available_ns,announced_ns";

    /** Not instantiated: this class only has static methods. */
    private Timeline()
    {
    }

    /**
     * Where a workload's timeline is kept.
     *
     * @param workload the workload
     * @param intake   the intake
     * @return the file
     */
    public static Path file(String workload, String intake)
    {
        return PerfSettings.out().resolve("timeline-" + workload + "-"
                + intake + ".csv");
    }

    /**
     * Writes a workload's timeline, in announcement order.
     *
     * @param workload the workload
     * @param intake   the intake
     * @param entries  its items
     * @throws IOException if it can't be written
     */
    public static void write(String workload, String intake,
            List<Entry> entries) throws IOException
    {
        Files.createDirectories(PerfSettings.out());
        StringBuilder csv = new StringBuilder(HEADER).append('\n');
        entries.stream().sorted(Comparator.comparingLong(Entry::announced))
                .forEach(entry -> csv.append(entry.seq()).append(',')
                        .append(entry.size()).append(',')
                        .append(entry.available()).append(',')
                        .append(entry.announced()).append('\n'));
        Files.writeString(file(workload, intake), csv, UTF_8);
    }

    /**
     * Reads a workload's timeline.
     *
     * @param workload the workload
     * @param intake   the intake
     * @return its items, in announcement order; empty if landing hasn't
     *         run for it
     * @throws IOException if it can't be read
     */
    public static Optional<List<Entry>> read(String workload, String intake)
            throws IOException
    {
        Path file = file(workload, intake);
        if (!Files.exists(file))
        {
            return Optional.empty();
        }
        List<Entry> entries = new ArrayList<>();
        List<String> lines = Files.readAllLines(file, UTF_8);
        for (String line : lines.subList(1, lines.size()))
        {
            String[] fields = line.split(",");
            entries.add(new Entry(Integer.parseInt(fields[0]),
                    Long.parseLong(fields[1]), Long.parseLong(fields[2]),
                    Long.parseLong(fields[3])));
        }
        return Optional.of(entries);
    }
}
