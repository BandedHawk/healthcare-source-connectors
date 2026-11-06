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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Where every stage writes its results (design §13.1), in
 * {@link PerfSettings#out()}:
 * <ul>
 * <li>{@code history.csv}: one row per stage, workload and intake per run,
 * kept until {@code mvn clean}, in the columns of {@link StageResult};
 * <li>{@code latest.csv}: the latest row of each, for the summary;
 * <li>{@code <stage>.md}: the stage's latest run, as tables;
 * <li>{@code pipeline-summary.md}: landing, splitting and end to end side
 * by side, from {@code latest.csv}.
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PerfOutput
{

    /** The run history. */
    public static final String HISTORY = "history.csv";

    /** The latest row of each stage, workload and intake. */
    public static final String LATEST = "latest.csv";

    /** The summary of the latest rows. */
    public static final String SUMMARY = "pipeline-summary.md";

    /** The stages in the summary's order. */
    private static final List<String> STAGES = List.of(PerfSettings.LANDING,
            PerfSettings.SPLIT, PerfSettings.END_TO_END, PerfSettings.CORE);

    /** Not instantiated: this class only has static methods. */
    private PerfOutput()
    {
    }

    /**
     * Records results: appends them to the history, replaces their rows in
     * {@code latest.csv}, writes the stage's report, prints it, and writes
     * the summary again.
     *
     * @param stage   the stage, for the report's name
     * @param results its results, in order
     * @param notes   lines for the report, after its tables
     * @throws IOException if a file can't be written
     */
    public static synchronized void record(String stage,
            List<StageResult> results, List<String> notes) throws IOException
    {
        Path out = Files.createDirectories(PerfSettings.out());
        String when = Instant.now().toString();
        String storage = PerfSettings.storage();
        StringBuilder rows = new StringBuilder();
        for (StageResult result : results)
        {
            rows.append(result.csvRow(when, storage)).append('\n');
        }
        Path history = out.resolve(HISTORY);
        if (Files.exists(history) && !Files.readAllLines(history, UTF_8)
                .getFirst().equals(StageResult.CSV_HEADER))
        {
            // Columns from an older version: kept beside the new history.
            Files.move(history, out.resolve("history-"
                    + when.replace(':', '-') + ".csv"),
                    StandardCopyOption.REPLACE_EXISTING);
        }
        if (!Files.exists(history))
        {
            Files.writeString(history, StageResult.CSV_HEADER + "\n", UTF_8);
        }
        Files.writeString(history, rows, UTF_8, StandardOpenOption.APPEND);

        Map<String, String> latest = readLatest(out);
        for (String row : rows.toString().split("\n"))
        {
            latest.put(key(row.split(",", -1)), row);
        }
        Files.writeString(out.resolve(LATEST), StageResult.CSV_HEADER + "\n"
                + String.join("\n", latest.values()) + "\n", UTF_8);

        String report = report(stage, results, notes);
        Files.writeString(out.resolve(stage + ".md"), report, UTF_8);
        writeSummary(out, latest);
        System.out.println("\n" + report);
        System.out.println("Written to " + out.resolve(stage + ".md") + ", "
                + out.resolve(HISTORY) + " and " + out.resolve(SUMMARY));
    }

    /**
     * The environment a run had, for a report's first line.
     *
     * @return the description
     */
    public static String environment()
    {
        return String.format(Locale.ROOT, "CPUs: %d, max heap: %d MiB,"
                + " Java %s, %s, storage: %s.",
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().maxMemory() >> 20,
                System.getProperty("java.version"),
                System.getProperty("os.arch"), PerfSettings.storage());
    }

    /**
     * A stage's report: its throughput, latency and memory tables.
     *
     * @param stage   the stage
     * @param results its results
     * @param notes   lines after the tables
     * @return the Markdown
     */
    static String report(String stage, List<StageResult> results,
            List<String> notes)
    {
        StringBuilder text = new StringBuilder();
        text.append("# Performance: ").append(stage).append(", ")
                .append(Instant.now()).append("\n\n")
                .append(environment()).append("\n\n");
        throughputTable(text, results);
        text.append("\n## Latency per item (ms)\n\n");
        text.append("| Stage | Workload | Intake | Items | p50 | p95 | p99"
                + " | Max |\n|---|---|---|---:|---:|---:|---:|---:|\n");
        results.forEach(result -> text.append(result.latencyRow())
                .append('\n'));
        text.append("\n## Memory\n\n");
        text.append("| Stage | Workload | Intake | Heap after GC, peak MiB"
                + " | Heap in use, peak MiB | Direct buffers, peak MiB"
                + " | Process RSS, peak MiB | Allocated GiB"
                + " | Allocation MiB/s | GC pause % | Full GCs |\n");
        text.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"
                + "\n");
        results.stream().map(StageResult::memoryRow)
                .filter(row -> row != null)
                .forEach(row -> text.append(row).append('\n'));
        text.append('\n');
        notes.forEach(note -> text.append(note).append('\n'));
        return text.toString();
    }

    /**
     * Appends the throughput table.
     *
     * @param text    where to write
     * @param results the rows
     */
    private static void throughputTable(StringBuilder text,
            List<StageResult> results)
    {
        text.append("## Throughput\n\n");
        text.append("| Stage | Workload | Intake | Items | MiB | Records"
                + " | Wall s | MiB/s | Items/s | Records/s | Avg cores"
                + " | Busiest thread | Stored ÷ source |\n");
        text.append("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:"
                + "|---|---:|\n");
        results.forEach(result -> text.append(result.throughputRow())
                .append('\n'));
    }

    /**
     * Writes {@code pipeline-summary.md}: for each workload, each stage's
     * latest throughput and latency through each intake, with when it ran;
     * the core rows, which have no intake, last.
     *
     * @param out    the output directory
     * @param latest the latest rows, by stage, workload and intake
     * @throws IOException if the file can't be written
     */
    static void writeSummary(Path out, Map<String, String> latest)
            throws IOException
    {
        Map<String, List<String[]>> byWorkload = new LinkedHashMap<>();
        for (String stage : STAGES)
        {
            for (String row : latest.values())
            {
                String[] fields = row.split(",", -1);
                if (fields[1].equals(stage))
                {
                    byWorkload.computeIfAbsent(fields[2],
                            key -> new ArrayList<>()).add(fields);
                }
            }
        }
        StringBuilder text = new StringBuilder();
        text.append("# Pipeline performance summary\n\n");
        text.append("The latest run of each stage, from ").append(LATEST)
                .append(". Landing and splitting each ran on their own;"
                        + " end to end is each item's landing latency plus"
                        + " its splitting latency, with the splitter fed"
                        + " each item when landing announced it (design"
                        + " §13.1). Core is the split, encrypt and store"
                        + " path without the apps. Rows from different runs"
                        + " compare only if they ran on the same machine"
                        + " and settings: check the time and the heap.\n");
        for (Map.Entry<String, List<String[]>> workload :
                byWorkload.entrySet())
        {
            text.append("\n## ").append(workload.getKey()).append("\n\n");
            text.append("| Stage | Intake | Items | MiB | Wall s | MiB/s"
                    + " | Items/s | p50 ms | p95 ms | p99 ms | Max ms"
                    + " | Heap after GC MiB | Avg cores | Max heap MiB"
                    + " | Ran at |\n");
            text.append("|---|---|---:|---:|---:|---:|---:|---:|---:|---:"
                    + "|---:|---:|---:|---:|---|\n");
            workload.getValue().sort(Comparator.comparing(
                    (String[] fields) -> fields[3].equals("-") ? "~"
                            : fields[3]).thenComparing(fields ->
                                    STAGES.indexOf(fields[1])));
            for (String[] fields : workload.getValue())
            {
                text.append(String.join(" | ", "| " + fields[1], fields[3],
                        fields[7],
                        fields[8], fields[10], fields[11], fields[12],
                        dash(fields[14]), dash(fields[15]), dash(fields[16]),
                        dash(fields[17]), dash(fields[22]),
                        dash(fields[18]), fields[5], seconds(fields[0])))
                        .append(" |\n");
            }
        }
        Files.writeString(out.resolve(SUMMARY), text.toString(), UTF_8);
    }

    /**
     * The latest rows, by stage, workload and intake, in the order first
     * recorded.
     *
     * @param out the output directory
     * @return the rows; empty if there are none yet
     * @throws IOException if the file can't be read
     */
    private static Map<String, String> readLatest(Path out)
            throws IOException
    {
        Map<String, String> latest = new LinkedHashMap<>();
        Path file = out.resolve(LATEST);
        if (Files.exists(file))
        {
            List<String> lines = Files.readAllLines(file, UTF_8);
            if (!lines.isEmpty()
                    && lines.getFirst().equals(StageResult.CSV_HEADER))
            {
                for (String line : lines.subList(1, lines.size()))
                {
                    if (!line.isBlank())
                    {
                        latest.put(key(line.split(",", -1)), line);
                    }
                }
            }
        }
        return latest;
    }

    /**
     * A row's stage, workload and intake.
     *
     * @param fields the row's fields
     * @return the key
     */
    private static String key(String[] fields)
    {
        return fields[1] + "|" + fields[2] + "|" + fields[3];
    }

    /**
     * A timestamp to the second.
     *
     * @param timestamp an ISO-8601 instant
     * @return it without its fraction of a second
     */
    private static String seconds(String timestamp)
    {
        int fraction = timestamp.indexOf('.');
        return fraction < 0 ? timestamp
                : timestamp.substring(0, fraction) + "Z";
    }

    /**
     * A field, or a dash where it is empty.
     *
     * @param field the field
     * @return what to show
     */
    private static String dash(String field)
    {
        return field.isEmpty() ? "–" : field;
    }
}
