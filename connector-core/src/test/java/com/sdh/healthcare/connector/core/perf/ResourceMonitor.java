/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.perf;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

/**
 * What a run costs the JVM it runs in: CPU, memory, allocation and GC,
 * measured the same way for every stage (design §13.1). Start it before
 * the run and stop it after; it measures the whole JVM, so a test's own
 * work, such as generating input, is counted too unless it is done
 * outside the run.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ResourceMonitor
{

    /**
     * What a run cost.
     *
     * @param wallNanos       the run's wall time
     * @param cores           CPU time over wall time: the cores used on
     *                        average
     * @param busiestThread   the busiest thread's name, without what varies
     *                        from run to run
     * @param busiestPercent  its CPU time as a share of wall time
     * @param heapAfterGcPeak the most heap in use right after a collection
     * @param heapInUsePeak   the most heap in use, sampled every 50 ms
     * @param directPeak      the most direct buffer memory, sampled
     * @param rssPeak         the most resident memory, sampled; -1 if
     *                        unknown
     * @param allocated       the bytes allocated
     * @param gcPauseMs       the stop-the-world GC time
     * @param fullGcs         the full collections
     */
    public record Resources(long wallNanos, double cores,
            String busiestThread, double busiestPercent,
            long heapAfterGcPeak, long heapInUsePeak, long directPeak,
            long rssPeak, long allocated, long gcPauseMs, long fullGcs)
    {

        /**
         * The share of wall time spent in GC pauses.
         *
         * @return a percentage
         */
        public double gcPercent()
        {
            return 100.0 * gcPauseMs * 1e6 / wallNanos;
        }

        /**
         * The allocation rate.
         *
         * @return MiB per second
         */
        public double allocationMibPerSecond()
        {
            return (allocated >> 20) / (wallNanos / 1e9);
        }
    }

    /** The thread bean, for allocation. */
    private final com.sun.management.ThreadMXBean threadBean =
            (com.sun.management.ThreadMXBean)
                    ManagementFactory.getThreadMXBean();

    /** The operating system bean, for process CPU. */
    private final OperatingSystemMXBean system = (OperatingSystemMXBean)
            ManagementFactory.getOperatingSystemMXBean();

    /** Samples the heap, direct buffers and RSS. */
    private final MemorySampler memory;

    /** Follows the heap after each collection. */
    private final HeapAfterGc afterGc;

    /** Each thread's CPU time at the start. */
    private final ThreadCpu threads;

    /** The bytes allocated at the start. */
    private final long allocatedBefore;

    /** The process CPU time at the start. */
    private final long cpuBefore;

    /** The GC pause time and full collections at the start. */
    private final long[] gcBefore;

    /** When the run started. */
    private final long start;

    /** Starts measuring. */
    private ResourceMonitor()
    {
        memory = new MemorySampler();
        afterGc = HeapAfterGc.start();
        allocatedBefore = threadBean.getTotalThreadAllocatedBytes();
        threads = ThreadCpu.start();
        cpuBefore = system.getProcessCpuTime();
        gcBefore = gcTotals();
        start = System.nanoTime();
    }

    /**
     * Collects garbage, so earlier work isn't counted, then starts
     * measuring.
     *
     * @return the monitor
     */
    public static ResourceMonitor start()
    {
        System.gc();
        return new ResourceMonitor();
    }

    /**
     * Stops measuring.
     *
     * @return what the run cost
     * @throws Exception if the sampling thread or the GC listener can't be
     *         stopped
     */
    public Resources stop() throws Exception
    {
        long wall = System.nanoTime() - start;
        long cpu = system.getProcessCpuTime() - cpuBefore;
        long allocated =
                threadBean.getTotalThreadAllocatedBytes() - allocatedBefore;
        MemorySampler.Peaks peaks = memory.stop();
        long heapAfterGcPeak = afterGc.stop();
        long[] gcAfter = gcTotals();
        ThreadCpu.Busiest busiest = threads.busiest(wall);
        return new Resources(wall, (double) cpu / wall, busiest.name(),
                busiest.percent(), heapAfterGcPeak, peaks.heap(),
                peaks.direct(), peaks.rss(), allocated,
                gcAfter[0] - gcBefore[0], gcAfter[1] - gcBefore[1]);
    }

    /**
     * Stop-the-world GC time in ms (G1's young and mixed pauses, plus any
     * full collection) and the number of full collections. G1's concurrent
     * cycle runs beside the application, so it isn't counted as pause
     * time.
     *
     * @return the pause time and the full collections
     */
    static long[] gcTotals()
    {
        long pauseMs = 0;
        long full = 0;
        for (GarbageCollectorMXBean collector :
                ManagementFactory.getGarbageCollectorMXBeans())
        {
            if (collector.getName().contains("Concurrent"))
            {
                continue;
            }
            pauseMs += Math.max(0, collector.getCollectionTime());
            if (collector.getName().contains("Old"))
            {
                full += Math.max(0, collector.getCollectionCount());
            }
        }
        return new long[] {pauseMs, full};
    }

    /**
     * CPU time per thread over a run. The busiest single thread's share of
     * wall time shows a single-threaded bottleneck (near 100%) that the
     * average across cores hides.
     */
    static final class ThreadCpu
    {

        /**
         * The busiest thread.
         *
         * @param name    its name, without a trailing number
         * @param percent its CPU time as a share of wall time
         */
        record Busiest(String name, double percent)
        {
        }

        /** The thread bean. */
        private final ThreadMXBean threadBean =
                ManagementFactory.getThreadMXBean();

        /** Each thread's CPU time at the start, by thread id. */
        private final Map<Long, Long> start = new HashMap<>();

        /** Creates the measurement; {@link #start} fills it. */
        private ThreadCpu()
        {
        }

        /**
         * Records every thread's CPU time now.
         *
         * @return the measurement
         */
        static ThreadCpu start()
        {
            ThreadCpu cpu = new ThreadCpu();
            for (long id : cpu.threadBean.getAllThreadIds())
            {
                cpu.start.put(id,
                        Math.max(0, cpu.threadBean.getThreadCpuTime(id)));
            }
            return cpu;
        }

        /**
         * The thread that used the most CPU since the start.
         *
         * @param wallNanos the wall time since the start
         * @return the busiest thread
         */
        Busiest busiest(long wallNanos)
        {
            String name = "?";
            long most = 0;
            for (ThreadInfo info :
                    threadBean.getThreadInfo(threadBean.getAllThreadIds()))
            {
                if (info == null)
                {
                    continue;
                }
                long used = threadBean.getThreadCpuTime(info.getThreadId())
                        - start.getOrDefault(info.getThreadId(), 0L);
                if (used > most)
                {
                    most = used;
                    name = shortName(info.getThreadName());
                }
            }
            return new Busiest(name, 100.0 * most / wallNanos);
        }

        /**
         * A thread's name without what varies from run to run: a trailing
         * number, a Camel thread's context and number, and the endpoint
         * after its scheme, which may name a directory.
         *
         * @param name the thread's name
         * @return the short name
         */
        static String shortName(String name)
        {
            return name.replaceAll(" - ([a-z0-9+.-]+):.*$", " - $1")
                    .replaceAll(" \\(camel-\\d+\\)", "")
                    .replaceAll(" #\\d+", "").replaceAll("-\\d+$", "");
        }
    }

    /**
     * Heap in use, direct buffers and the process's resident memory,
     * sampled every 50 ms. Sampled as totals: the sum of per-pool peaks
     * would overstate the heap, as pools peak at different times.
     */
    static final class MemorySampler
    {

        /**
         * The peaks seen.
         *
         * @param heap   heap in use
         * @param direct direct buffer memory
         * @param rss    resident set size, or -1 if unknown
         */
        record Peaks(long heap, long direct, long rss)
        {
        }

        /** The process's memory statistics (Linux). */
        private static final Path STATM = Path.of("/proc/self/statm");

        /** The page size statm counts in. */
        private static final long PAGE = 4096;

        /** The sampling thread. */
        private final Thread thread;

        /** Cleared to stop sampling. */
        private volatile boolean running = true;

        /** The peak heap in use. */
        private volatile long heap;

        /** The peak direct buffer memory. */
        private volatile long direct;

        /** The peak resident set size, or -1 if unknown. */
        private volatile long rss = -1;

        /** Starts sampling. */
        MemorySampler()
        {
            MemoryMXBean bean = ManagementFactory.getMemoryMXBean();
            BufferPoolMXBean directPool = ManagementFactory
                    .getPlatformMXBeans(BufferPoolMXBean.class).stream()
                    .filter(pool -> pool.getName().equals("direct"))
                    .findFirst().orElse(null);
            thread = Thread.ofPlatform().daemon().name("memory-sampler")
                    .start(() ->
                    {
                        while (running)
                        {
                            heap = Math.max(heap,
                                    bean.getHeapMemoryUsage().getUsed());
                            if (directPool != null)
                            {
                                direct = Math.max(direct,
                                        directPool.getMemoryUsed());
                            }
                            rss = Math.max(rss, residentBytes());
                            try
                            {
                                Thread.sleep(50);
                            }
                            catch (InterruptedException interrupted)
                            {
                                return;
                            }
                        }
                    });
        }

        /**
         * Resident set size from /proc (Linux, 4 KiB pages).
         *
         * @return bytes, or -1 elsewhere
         */
        static long residentBytes()
        {
            try
            {
                return Long.parseLong(Files.readString(STATM).strip()
                        .split("\\s+")[1]) * PAGE;
            }
            catch (IOException | RuntimeException failure)
            {
                return -1;
            }
        }

        /**
         * Stops sampling.
         *
         * @return the peaks seen
         * @throws InterruptedException if interrupted while waiting for the
         *         sampling thread
         */
        Peaks stop() throws InterruptedException
        {
            running = false;
            thread.join();
            return new Peaks(heap, direct, rss);
        }
    }

    /**
     * The most heap in use right after any collection during the run, from
     * the collectors' notifications.
     */
    static final class HeapAfterGc implements NotificationListener
    {

        /** The names of the heap's memory pools. */
        private final Set<String> heapPools = new HashSet<>();

        /** The peak heap in use after a collection. */
        private final AtomicLong peak = new AtomicLong();

        /** Creates the listener; {@link #start} registers it. */
        private HeapAfterGc()
        {
        }

        /**
         * Starts listening to every collector.
         *
         * @return the listener
         */
        static HeapAfterGc start()
        {
            HeapAfterGc listener = new HeapAfterGc();
            for (MemoryPoolMXBean pool :
                    ManagementFactory.getMemoryPoolMXBeans())
            {
                if (pool.getType() == MemoryType.HEAP)
                {
                    listener.heapPools.add(pool.getName());
                }
            }
            for (GarbageCollectorMXBean collector :
                    ManagementFactory.getGarbageCollectorMXBeans())
            {
                ((NotificationEmitter) collector)
                        .addNotificationListener(listener, null, null);
            }
            return listener;
        }

        /**
         * Records the heap in use after a collection.
         *
         * @param notification the collector's notification
         * @param handback     unused
         */
        @Override
        public void handleNotification(Notification notification,
                Object handback)
        {
            if (!notification.getType().equals(GarbageCollectionNotificationInfo
                    .GARBAGE_COLLECTION_NOTIFICATION))
            {
                return;
            }
            GcInfo info = GarbageCollectionNotificationInfo.from(
                    (CompositeData) notification.getUserData()).getGcInfo();
            long used = info.getMemoryUsageAfterGc().entrySet().stream()
                    .filter(entry -> heapPools.contains(entry.getKey()))
                    .mapToLong(entry -> entry.getValue().getUsed()).sum();
            peak.accumulateAndGet(used, Math::max);
        }

        /**
         * Stops listening.
         *
         * @return the peak heap in use after a collection
         * @throws Exception if a listener can't be removed
         */
        long stop() throws Exception
        {
            for (GarbageCollectorMXBean collector :
                    ManagementFactory.getGarbageCollectorMXBeans())
            {
                ((NotificationEmitter) collector)
                        .removeNotificationListener(this);
            }
            return peak.get();
        }
    }
}
