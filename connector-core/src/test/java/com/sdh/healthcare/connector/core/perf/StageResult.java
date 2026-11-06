/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.perf;

import java.util.Locale;

/**
 * One stage's result for one workload, in the columns every stage shares
 * (design §13.1), so landing, splitting and end to end can be set side by
 * side.
 *
 * @param stage       {@link PerfSettings#CORE}, {@link PerfSettings#LANDING},
 *                    {@link PerfSettings#SPLIT} or
 *                    {@link PerfSettings#END_TO_END}
 * @param workload    the workload, such as {@link PerfSettings#BULK_CCDA}
 * @param intake      the landing intake the items came through; {@code -}
 *                    for none
 * @param items       how many items
 * @param bytes       their total size
 * @param records     the records split from them; -1 where not split
 * @param wallNanos   the run's wall time, from the first item available to
 *                    the last done
 * @param latency     the per-item latencies; null where not measured
 * @param resources   what the run cost the JVM; null where not measured
 * @param storedRatio the bytes stored over the bytes in; NaN where not
 *                   measured
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record StageResult(String stage, String workload, String intake,
        long items, long bytes, long records, long wallNanos,
        Latencies.Summary latency, ResourceMonitor.Resources resources,
        double storedRatio)
{

    /** The columns of {@code history.csv} and {@code latest.csv}. */
    public static final String CSV_HEADER = "timestamp,stage,workload,"
            + "intake,storage,max_heap_mib,cpus,items,size_mib,records,"
            + "wall_s,mib_per_s,items_per_s,records_per_s,"
            + "latency_p50_ms,latency_p95_ms,latency_p99_ms,latency_max_ms,"
            + "avg_cores,busiest_thread,busiest_pct,stored_ratio,"
            + "heap_after_gc_peak_mib,heap_in_use_peak_mib,direct_peak_mib,"
            + "rss_peak_mib,allocated_gib,allocation_mib_per_s,gc_pause_pct,"
            + "full_gcs";

    /**
     * The input's size.
     *
     * @return MiB
     */
    public double mib()
    {
        return bytes / 1048576.0;
    }

    /**
     * The wall time.
     *
     * @return seconds
     */
    public double seconds()
    {
        return wallNanos / 1e9;
    }

    /**
     * A row of the throughput table.
     *
     * @return the Markdown row
     */
    public String throughputRow()
    {
        return String.format(Locale.ROOT,
                "| %s | %s | %s | %,d | %.0f | %s | %.1f | %.1f | %,.1f"
                        + " | %s | %s | %s | %s |",
                stage, workload, intake, items, mib(),
                records < 0 ? "–" : String.format(Locale.ROOT, "%,d",
                        records),
                seconds(), mib() / seconds(), items / seconds(),
                records < 0 ? "–" : String.format(Locale.ROOT, "%,.0f",
                        records / seconds()),
                resources == null ? "–" : String.format(Locale.ROOT,
                        "%.1f", resources.cores()),
                resources == null ? "–" : String.format(Locale.ROOT,
                        "%s %.0f%%", resources.busiestThread(),
                        resources.busiestPercent()),
                Double.isNaN(storedRatio) ? "–" : String.format(Locale.ROOT,
                        "%.2f", storedRatio));
    }

    /**
     * A row of the latency table.
     *
     * @return the Markdown row
     */
    public String latencyRow()
    {
        return latency == null
                ? String.format(Locale.ROOT, "| %s | %s | %s | – | – | – | –"
                        + " | – |", stage, workload, intake)
                : String.format(Locale.ROOT, "| %s | %s | %s | %,d | %,.0f"
                        + " | %,.0f | %,.0f | %,.0f |", stage, workload,
                        intake, latency.count(), latency.p50(), latency.p95(),
                        latency.p99(), latency.max());
    }

    /**
     * A row of the memory table.
     *
     * @return the Markdown row; null where not measured
     */
    public String memoryRow()
    {
        if (resources == null)
        {
            return null;
        }
        return String.format(Locale.ROOT,
                "| %s | %s | %s | %d | %d | %d | %s | %.1f | %,.0f | %.1f"
                        + " | %d |", stage, workload, intake,
                resources.heapAfterGcPeak() >> 20,
                resources.heapInUsePeak() >> 20,
                resources.directPeak() >> 20,
                resources.rssPeak() < 0 ? "n/a"
                        : Long.toString(resources.rssPeak() >> 20),
                resources.allocated() / 1073741824.0,
                resources.allocationMibPerSecond(), resources.gcPercent(),
                resources.fullGcs());
    }

    /**
     * A row of {@code history.csv}.
     *
     * @param when    when the run ended
     * @param storage where the tables were
     * @return the CSV row; empty fields where not measured
     */
    public String csvRow(String when, String storage)
    {
        StringBuilder row = new StringBuilder();
        row.append(when).append(',').append(stage).append(',')
                .append(workload).append(',').append(intake).append(',')
                .append(storage).append(',')
                .append(Runtime.getRuntime().maxMemory() >> 20).append(',')
                .append(Runtime.getRuntime().availableProcessors())
                .append(',').append(items).append(',')
                .append(number("%.1f", mib())).append(',')
                .append(records < 0 ? "" : Long.toString(records))
                .append(',').append(number("%.2f", seconds())).append(',')
                .append(number("%.2f", mib() / seconds())).append(',')
                .append(number("%.2f", items / seconds())).append(',')
                .append(records < 0 ? ""
                        : number("%.0f", records / seconds()))
                .append(',');
        if (latency == null)
        {
            row.append(",,,,");
        }
        else
        {
            row.append(number("%.1f", latency.p50())).append(',')
                    .append(number("%.1f", latency.p95())).append(',')
                    .append(number("%.1f", latency.p99())).append(',')
                    .append(number("%.1f", latency.max())).append(',');
        }
        if (resources == null)
        {
            row.append(",,,");
        }
        else
        {
            row.append(number("%.2f", resources.cores())).append(',')
                    .append(resources.busiestThread()).append(',')
                    .append(number("%.0f", resources.busiestPercent()))
                    .append(',');
        }
        row.append(Double.isNaN(storedRatio) ? ""
                : number("%.3f", storedRatio)).append(',');
        if (resources == null)
        {
            row.append(",,,,,,,");
        }
        else
        {
            row.append(resources.heapAfterGcPeak() >> 20).append(',')
                    .append(resources.heapInUsePeak() >> 20).append(',')
                    .append(resources.directPeak() >> 20).append(',')
                    .append(resources.rssPeak() < 0 ? -1
                            : resources.rssPeak() >> 20).append(',')
                    .append(number("%.2f",
                            resources.allocated() / 1073741824.0))
                    .append(',')
                    .append(number("%.0f",
                            resources.allocationMibPerSecond()))
                    .append(',')
                    .append(number("%.2f", resources.gcPercent()))
                    .append(',').append(resources.fullGcs());
        }
        return row.toString();
    }

    /**
     * Formats a number the same way on every machine.
     *
     * @param format the format
     * @param value  the number
     * @return the text
     */
    private static String number(String format, double value)
    {
        return String.format(Locale.ROOT, format, value);
    }
}
