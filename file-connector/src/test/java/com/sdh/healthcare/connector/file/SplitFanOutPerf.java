/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.broker.receive.BatchHandler.Outcome;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.perf.Latencies;
import com.sdh.healthcare.connector.core.perf.PerfSettings;
import com.sdh.healthcare.connector.core.perf.PerfStorage;
import com.sdh.healthcare.connector.core.perf.ResourceMonitor;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.file.split.SplitHandler;
import com.sdh.healthcare.connector.runtime.FatalExit;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * The shape of the split stage's cost as an item holds more records
 * (docs/PERFORMANCE.md §13): CSV items of 1 to 100,000 rows, split by the
 * real split stage, in process, on embedded Postgres and the
 * {@code perf.storage} tables, with a Camel queue standing in for its
 * output broker, as {@link SplitterPerf} does.
 * <p>
 * Each point is a number of rows an item, a row's size, a batch size
 * ({@code sdh.storage.commit.max-records}), a number of items split at once
 * and the audit journal on or off. Its items are landed first, untimed,
 * then handed to the stage all at once, as many at a time as the
 * concurrency. For each point it records:
 * <ul>
 * <li>the items and records a second, and each item's time from its
 * {@code item.landed} handed over to its {@code item.split} sent;</li>
 * <li>the stage's own timers ({@code sdh.stage.phase_duration}), each
 * summed over the run and given per item and per record: reading and
 * splitting the records ({@code records_read}), encrypting and writing
 * them to the data file ({@code records_store}), closing the data file
 * ({@code write}), committing it ({@code prepare}, {@code append},
 * {@code save}), and the item's own steps ({@code refs},
 * {@code announce}, {@code archive}, {@code checkpoint}, and so on);</li>
 * <li>what Postgres did for it: transactions, rows written, write-ahead
 * log and the CPU its processes used.</li>
 * </ul>
 * The rows are synthetic: a sequence number and letters, no PHI. The
 * broker is a queue in memory, so a message's cost here is its sealing,
 * not a broker's acknowledgement.
 * <p>
 * Run with {@code ./mvnw -Pperf -pl file-connector -am verify
 * -Dperf.stages=fanout}, and {@code -Dperf.fanout.rows=1,100,10000},
 * {@code -Dperf.fanout.row-bytes=200}, {@code -Dperf.fanout.records=20000}
 * (records a point, at least one item), {@code -Dperf.fanout.max-items=2000},
 * {@code -Dperf.fanout.concurrency=64}, {@code -Dperf.fanout.batch=10000}
 * and {@code -Dperf.fanout.audit=on,off,on:100} (on, with a 100 ms linger)
 * to choose the points; {@code -Dperf.fanout.pool-size=2} sets the stage's
 * registry pool ({@code sdh.lease.jdbc.pool-size}, docs/PERFORMANCE.md
 * §13.3) for every point. The
 * results go to {@code target/perf/split-fanout-<time>.md}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SplitFanOutPerf
{

    /** The stage name this test runs under, in {@code perf.stages}. */
    static final String STAGE = "fanout";

    /**
     * Where the splitter's messages go: a bounded queue that holds the
     * splitter up when full, as a slow broker would, read by 16 threads,
     * so that it can't be what sets the pace (with one, the pace was the
     * same: sealing the messages sets it, docs/PERFORMANCE.md §13).
     */
    static final String OUT_QUEUE = "seda:perf-fanout-out?size=10000"
            + "&blockWhenFull=true&concurrentConsumers=16";

    /** How long a point may take, at most. */
    static final Duration RUN_LIMIT = Duration.ofMinutes(60);

    /** How long Postgres' backends take to report their statistics. */
    static final Duration STATS_SETTLE = Duration.ofSeconds(2);

    /** The phase timer's name. */
    static final String PHASE_METRIC = "sdh.stage.phase_duration";

    /** When each item's {@code item.split} was published, by item. */
    static final Map<UUID, Long> COMPLETED = new ConcurrentHashMap<>();

    /** The records split in the current point. */
    static final AtomicLong RECORDS = new AtomicLong();

    /** The reasons given to the exit hook. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** The current point's metric reader, made with its splitter. */
    static volatile InMemoryMetricReader reader;

    /** The tables' storage, for every point. */
    static PerfStorage storage;

    /** The test's files. */
    static Path root;

    /** The key files. */
    static TestKeys keys;

    /** The points' results, in order. */
    static final List<Point> POINTS = new ArrayList<>();

    /**
     * Each point's statements, by its name, with
     * {@code -Dsdh.test.pg.statements=true}: the commonest, as
     * {@code pg_stat_statements} normalises them (no values), with their
     * calls an item.
     */
    static final Map<String, List<String>> STATEMENTS =
            new LinkedHashMap<>();

    /** The most statements listed a point. */
    static final int STATEMENTS_LISTED = 40;

    /** Creates the tests; JUnit runs each test on a new instance. */
    SplitFanOutPerf()
    {
    }

    /**
     * What a point is.
     *
     * @param rows        rows an item
     * @param rowBytes    a row's length, its newline included
     * @param items       how many items
     * @param concurrency how many items are split at once, at most
     * @param batch       the most records in a commit batch
     * @param linger      the audit journal's linger, in milliseconds;
     *                    -1 for no journal
     */
    record Shape(int rows, int rowBytes, int items, int concurrency,
            int batch, int linger)
    {

        /**
         * Whether the audit journal is kept.
         *
         * @return true if it is
         */
        boolean audit()
        {
            return linger >= 0;
        }

        /**
         * The audit setting, for the report.
         *
         * @return {@code off}, {@code on}, or {@code on+<linger>ms}
         */
        String auditName()
        {
            return linger < 0 ? "off" : linger == 0 ? "on"
                    : "on+" + linger + "ms";
        }

        /**
         * The point's name, for its directory and intake.
         *
         * @return the name
         */
        String name()
        {
            return "fanout-" + rows + "x" + rowBytes + "-c" + concurrency
                    + "-b" + batch + "-audit-" + auditName();
        }
    }

    /**
     * A phase's time over a point.
     *
     * @param count how many times it was timed
     * @param sum   the total, in seconds
     */
    record Phase(long count, double sum)
    {
    }

    /**
     * What Postgres had done, from its statistics and processes.
     *
     * @param transactions transactions committed and rolled back
     * @param inserted     rows inserted
     * @param updated      rows updated
     * @param deleted      rows deleted
     * @param walBytes     the write-ahead log's position, in bytes
     * @param cpuSeconds   the CPU its processes have used; NaN if unknown
     */
    record Postgres(long transactions, long inserted, long updated,
            long deleted, long walBytes, double cpuSeconds)
    {

        /**
         * What was done between this and a later reading.
         *
         * @param later the later reading
         * @return the difference
         */
        Postgres to(Postgres later)
        {
            return new Postgres(later.transactions - transactions,
                    later.inserted - inserted, later.updated - updated,
                    later.deleted - deleted, later.walBytes - walBytes,
                    later.cpuSeconds - cpuSeconds);
        }
    }

    /**
     * A point's results.
     *
     * @param shape     what it was
     * @param records   the records split
     * @param bytes     the items' bytes
     * @param wallNanos from the first item handed over to the last
     *                  {@code item.split}
     * @param latency   each item's time to its {@code item.split}
     * @param resources what the JVM used
     * @param phases    the stage's timers, by phase
     * @param postgres  what Postgres did
     */
    record Point(Shape shape, long records, long bytes, long wallNanos,
            Latencies.Summary latency, ResourceMonitor.Resources resources,
            Map<String, Phase> phases, Postgres postgres)
    {
    }

    /** The beans the splitter gets for the test. */
    @Configuration(proxyBeanMethods = false)
    static class PerfBeans
    {

        /** Created by Spring. */
        PerfBeans()
        {
        }

        /**
         * A recording exit hook in place of the real one.
         *
         * @return the hook
         */
        @Bean
        @Primary
        FatalExit recordingFatalExit()
        {
            return FATAL::add;
        }

        /**
         * A meter provider whose metrics the test reads, for the stage's
         * timers; Spring Boot's OpenTelemetry SDK takes it up.
         *
         * @return the provider
         */
        @Bean(destroyMethod = "close")
        SdkMeterProvider perfMeterProvider()
        {
            InMemoryMetricReader made = InMemoryMetricReader.create();
            reader = made;
            return SdkMeterProvider.builder().registerMetricReader(made)
                    .build();
        }

        /**
         * The broker stand-in: counts records and notes when each item's
         * {@code item.split} came, by the unencrypted headers, and drops
         * the messages.
         *
         * @return the route
         */
        @Bean
        RouteBuilder perfBroker()
        {
            return new RouteBuilder()
            {
                @Override
                public void configure()
                {
                    from(OUT_QUEUE).routeId("perf-fanout-broker")
                            .process(exchange ->
                            {
                                long at = System.nanoTime();
                                String type = exchange.getIn().getHeader(
                                        "type", String.class);
                                if (MessageType.ITEM_SPLIT.wireName()
                                        .equals(type))
                                {
                                    COMPLETED.put(UUID.fromString(exchange
                                            .getIn().getHeader("itemId",
                                                    String.class)), at);
                                }
                                else if (MessageType.RECORD_SPLIT.wireName()
                                        .equals(type))
                                {
                                    RECORDS.incrementAndGet();
                                }
                            });
                }
            };
        }
    }

    /**
     * Starts the storage and makes the keys, if this test is to run.
     *
     * @throws Exception if one can't be made
     */
    @BeforeAll
    static void start() throws Exception
    {
        Assumptions.assumeTrue(PerfSettings.runs(STAGE),
                "perf.stages doesn't include " + STAGE);
        storage = PerfStorage.create(PerfSettings.storage());
        root = Files.createDirectories(PerfSettings.out().resolve("work")
                .resolve(STAGE));
        SplitterPerf.clear(root);
        keys = new TestKeys(Files.createDirectories(root.resolve("keys")));
    }

    /**
     * Writes the results, and closes the storage.
     *
     * @throws Exception if the results can't be written
     */
    @AfterAll
    static void finish() throws Exception
    {
        if (storage == null)
        {
            return;
        }
        try
        {
            if (!POINTS.isEmpty())
            {
                Path report = PerfSettings.out().resolve("split-fanout-"
                        + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                                .format(Instant.now().atOffset(
                                        ZoneOffset.UTC)) + ".md");
                String text = report(storage.describe());
                Files.writeString(report, text);
                System.out.println(text);
                System.out.println("Written to " + report);
            }
        }
        finally
        {
            storage.close();
            SplitterPerf.clear(root);
        }
    }

    /**
     * Splits each point in turn.
     *
     * @throws Exception if a point fails
     */
    @Test
    void fanOut() throws Exception
    {
        int records = Integer.getInteger("perf.fanout.records", 20_000);
        int maxItems = Integer.getInteger("perf.fanout.max-items", 2_000);
        // Warms the JIT up, and isn't kept.
        run(new Shape(10, 200, 200, 64, 10_000, 0));
        for (int linger : audits())
        {
            for (int rowBytes : ints("perf.fanout.row-bytes", "200,2000"))
            {
                for (int concurrency : ints("perf.fanout.concurrency", "64"))
                {
                    for (int batch : ints("perf.fanout.batch", "10000"))
                    {
                        for (int rows : ints("perf.fanout.rows",
                                "1,10,100,1000,10000,100000"))
                        {
                            int items = (int) Math.min(maxItems,
                                    Math.max(1, Math.ceilDiv(records, rows)));
                            POINTS.add(run(new Shape(rows, rowBytes, items,
                                    concurrency, batch, linger)));
                        }
                    }
                }
            }
        }
    }

    /**
     * One point: a splitter of its own splits its items.
     *
     * @param shape what the point is
     * @return its results
     * @throws Exception if it fails
     */
    Point run(Shape shape) throws Exception
    {
        COMPLETED.clear();
        RECORDS.set(0);
        FATAL.clear();
        String name = shape.name();
        Path work = Files.createDirectories(root.resolve(name));
        try
        {
            Postgres before;
            Postgres after;
            Point point;
            try (ConfigurableApplicationContext splitter =
                    new SpringApplicationBuilder(
                            FileConnectorApplication.class, PerfBeans.class)
                            .run(arguments(shape, name)))
            {
                ConnectorRuntime runtime =
                        splitter.getBean(ConnectorRuntime.class);
                // The items, landed as the landing stage would: untimed.
                TestLanding landing = new TestLanding(
                        storage.catalogProperties(), runtime.keys().wrap(),
                        keys, name);
                List<LandedItem> items = new ArrayList<>();
                Random random = new Random(shape.rows() * 31L
                        + shape.rowBytes());
                for (int index = 0; index < shape.items(); index++)
                {
                    items.add(landing.land(csv(shape, index, random),
                            null));
                }
                long bytes = items.stream().mapToLong(LandedItem::size)
                        .sum();
                List<byte[]> announcements = items.stream()
                        .map(landing::announcement).toList();
                SplitHandler handler = new SplitHandler(runtime.splitter(),
                        runtime.keys().inbox(), name, TestLanding.CATALOG,
                        LandingTables.DEFAULT_NAMESPACE, FATAL::add);
                ExecutorService input = Executors.newFixedThreadPool(
                        shape.concurrency());
                Thread.sleep(STATS_SETTLE.toMillis());
                Map<String, Phase> phasesBefore = phases();
                resetStatements();
                before = postgres();
                ResourceMonitor monitor = ResourceMonitor.start();
                long start = System.nanoTime();
                long[] sent = new long[items.size()];
                List<Future<Outcome>> outcomes = new ArrayList<>();
                for (int seq = 0; seq < items.size(); seq++)
                {
                    int at = seq;
                    byte[] body = announcements.get(seq);
                    outcomes.add(input.submit(() ->
                    {
                        sent[at] = System.nanoTime();
                        return handler.handle(List.of(new BrokerMessage(
                                body, UUID.randomUUID().toString(),
                                Map.of())));
                    }));
                }
                long deadline = System.nanoTime() + RUN_LIMIT.toNanos();
                while (COMPLETED.size() < items.size()
                        && System.nanoTime() < deadline && FATAL.isEmpty())
                {
                    Thread.sleep(5);
                }
                ResourceMonitor.Resources resources = monitor.stop();
                for (Future<Outcome> outcome : outcomes)
                {
                    assertThat(outcome.get()).isEqualTo(Outcome.DONE);
                }
                input.shutdown();
                assertThat(FATAL).as("fatal exits").isEmpty();
                assertThat(COMPLETED).as("item.split messages")
                        .hasSize(items.size());
                Latencies latency = new Latencies();
                long last = start;
                for (int seq = 0; seq < items.size(); seq++)
                {
                    long completed = COMPLETED.get(items.get(seq).itemId());
                    latency.add(completed - sent[seq]);
                    last = Math.max(last, completed);
                }
                Thread.sleep(STATS_SETTLE.toMillis());
                after = postgres();
                STATEMENTS.put(name, statements(items.size()));
                point = new Point(shape, RECORDS.get(), bytes, last - start,
                        latency.summary(), resources,
                        minus(phases(), phasesBefore), before.to(after));
            }
            System.out.println(line(point));
            return point;
        }
        finally
        {
            storage.dropNamespace(SplitterPerf.NAMESPACE);
            storage.dropNamespace(LandingTables.DEFAULT_NAMESPACE);
            SplitterPerf.clear(work);
            Files.deleteIfExists(work);
        }
    }

    /**
     * An item's CSV: its rows, each a sequence number and letters to the
     * row's length, newline included.
     *
     * @param shape  the point
     * @param item   the item's number
     * @param random where the letters come from
     * @return the item's bytes
     */
    static byte[] csv(Shape shape, int item, Random random)
    {
        int length = Math.max(16, shape.rowBytes());
        byte[] bytes = new byte[length * shape.rows()];
        for (int row = 0; row < shape.rows(); row++)
        {
            int at = row * length;
            byte[] key = String.format("%08d,%06d,", item, row % 1_000_000)
                    .getBytes(US_ASCII);
            System.arraycopy(key, 0, bytes, at, key.length);
            for (int index = key.length; index < length - 1; index++)
            {
                bytes[at + index] = (byte) ('a' + random.nextInt(26));
            }
            bytes[at + length - 1] = '\n';
        }
        return bytes;
    }

    /**
     * The split stage's timers so far, by phase.
     *
     * @return each phase's count and total
     */
    static Map<String, Phase> phases()
    {
        Map<String, Phase> phases = new TreeMap<>();
        InMemoryMetricReader current = reader;
        if (current == null)
        {
            return phases;
        }
        for (MetricData metric : current.collectAllMetrics())
        {
            if (!metric.getName().equals(PHASE_METRIC))
            {
                continue;
            }
            for (HistogramPointData point
                    : metric.getHistogramData().getPoints())
            {
                String stage = point.getAttributes().get(
                        AttributeKey.stringKey(Attr.STAGE.key()));
                String phase = point.getAttributes().get(
                        AttributeKey.stringKey(Attr.PHASE.key()));
                if ("split".equals(stage) && phase != null)
                {
                    phases.merge(phase, new Phase(point.getCount(),
                            point.getSum()), (one, two) -> new Phase(
                            one.count() + two.count(), one.sum() + two.sum()));
                }
            }
        }
        return phases;
    }

    /**
     * The phases' times between two readings.
     *
     * @param later   the later reading
     * @param earlier the earlier
     * @return the difference, by phase
     */
    static Map<String, Phase> minus(Map<String, Phase> later,
            Map<String, Phase> earlier)
    {
        Map<String, Phase> phases = new TreeMap<>();
        later.forEach((phase, times) ->
        {
            Phase before = earlier.getOrDefault(phase, new Phase(0, 0));
            if (times.count() > before.count())
            {
                phases.put(phase, new Phase(times.count() - before.count(),
                        times.sum() - before.sum()));
            }
        });
        return phases;
    }

    /**
     * What Postgres has done so far: its statistics, summed over its
     * databases, its log's position, and its processes' CPU.
     *
     * @return the reading
     * @throws Exception if it can't be read
     */
    static Postgres postgres() throws Exception
    {
        try (Connection connection = TestPostgres.dataSource()
                .getConnection();
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("""
                        SELECT sum(xact_commit + xact_rollback),
                               sum(tup_inserted), sum(tup_updated),
                               sum(tup_deleted),
                               pg_wal_lsn_diff(pg_current_wal_lsn(),
                                               '0/0')::bigint
                          FROM pg_stat_database"""))
        {
            rows.next();
            return new Postgres(rows.getLong(1), rows.getLong(2),
                    rows.getLong(3), rows.getLong(4), rows.getLong(5),
                    postgresCpu(TestPostgres.get().getPort()));
        }
    }

    /**
     * Clears {@code pg_stat_statements}, when it is loaded.
     *
     * @throws Exception if it can't be cleared
     */
    static void resetStatements() throws Exception
    {
        if (!Boolean.getBoolean(TestPostgres.STATEMENTS))
        {
            return;
        }
        try (Connection connection = TestPostgres.dataSource()
                .getConnection();
                Statement statement = connection.createStatement())
        {
            statement.execute(
                    "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
            statement.execute("SELECT pg_stat_statements_reset()");
        }
    }

    /**
     * The commonest statements since {@link #resetStatements}, each as a
     * table row: its calls an item, its mean milliseconds and its text on
     * one line; none unless {@code pg_stat_statements} is loaded.
     *
     * @param items the point's items
     * @return the rows
     * @throws Exception if they can't be read
     */
    static List<String> statements(int items) throws Exception
    {
        List<String> rows = new ArrayList<>();
        if (!Boolean.getBoolean(TestPostgres.STATEMENTS))
        {
            return rows;
        }
        try (Connection connection = TestPostgres.dataSource()
                .getConnection();
                Statement statement = connection.createStatement();
                ResultSet found = statement.executeQuery("""
                        SELECT calls, mean_exec_time,
                               regexp_replace(query, '\\s+', ' ', 'g')
                          FROM pg_stat_statements
                         ORDER BY calls DESC
                         LIMIT """ + " " + STATEMENTS_LISTED))
        {
            while (found.next())
            {
                rows.add(String.format("| %.2f | %.3f | `%s` |",
                        (double) found.getLong(1) / items,
                        found.getDouble(2),
                        found.getString(3).replace("|", "\\|")));
            }
        }
        return rows;
    }

    /**
     * The CPU the embedded Postgres has used: its postmaster's, with its
     * ended children's, and its running children's. Linux only.
     *
     * @param port the port it listens on
     * @return seconds; NaN if it can't be found
     */
    static double postgresCpu(int port)
    {
        Path proc = Path.of("/proc");
        if (!Files.isDirectory(proc))
        {
            return Double.NaN;
        }
        long ticksPerSecond = 100;
        long postmaster = -1;
        Map<Long, long[]> stats = new LinkedHashMap<>();
        try (Stream<Path> entries = Files.list(proc))
        {
            for (Path entry : entries.toList())
            {
                String pid = entry.getFileName().toString();
                if (!pid.chars().allMatch(Character::isDigit))
                {
                    continue;
                }
                try
                {
                    String stat = Files.readString(entry.resolve("stat"));
                    // After the command's closing bracket: state, ppid, ...
                    String[] fields = stat.substring(
                            stat.lastIndexOf(')') + 2).split(" ");
                    long ppid = Long.parseLong(fields[1]);
                    long own = Long.parseLong(fields[11])
                            + Long.parseLong(fields[12]);
                    long ended = Long.parseLong(fields[13])
                            + Long.parseLong(fields[14]);
                    stats.put(Long.parseLong(pid),
                            new long[] {ppid, own, ended});
                    if (postmaster < 0 && stat.contains("(postgres)"))
                    {
                        List<String> command = Arrays.asList(Files
                                .readString(entry.resolve("cmdline"))
                                .split("\0"));
                        int flag = command.indexOf("-p");
                        if (flag >= 0 && flag + 1 < command.size()
                                && command.get(flag + 1)
                                        .equals(String.valueOf(port)))
                        {
                            postmaster = Long.parseLong(pid);
                        }
                    }
                }
                catch (IOException | RuntimeException gone)
                {
                    // Ended while being read, or not ours to read.
                }
            }
        }
        catch (IOException unreadable)
        {
            return Double.NaN;
        }
        long[] main = stats.get(postmaster);
        if (main == null)
        {
            return Double.NaN;
        }
        long ticks = main[1] + main[2];
        for (long[] child : stats.values())
        {
            if (child[0] == postmaster)
            {
                ticks += child[1];
            }
        }
        return (double) ticks / ticksPerSecond;
    }

    /**
     * The split stage's settings for a point: {@link SplitterPerf}'s, for
     * CSV, with the point's batch, concurrency and audit journal.
     *
     * @param shape the point
     * @param name  the intake name the items landed under
     * @return the arguments
     * @throws Exception if the recipient's fingerprint can't be made
     */
    static String[] arguments(Shape shape, String name) throws Exception
    {
        List<String> arguments = new ArrayList<>();
        for (String argument : SplitterPerf.arguments("csv", name, keys,
                storage))
        {
            if (!argument.startsWith("--sdh.split.format=")
                    && !argument.startsWith("--sdh.stage.output.uri="))
            {
                arguments.add(argument);
            }
        }
        arguments.add("--sdh.split.format=csv");
        arguments.add("--sdh.stage.output.uri=" + OUT_QUEUE);
        arguments.add("--sdh.split.concurrency=" + shape.concurrency());
        arguments.add("--sdh.split.consumers=1");
        arguments.add("--sdh.storage.commit.max-records=" + shape.batch());
        arguments.add("--sdh.telemetry.journal.enabled=" + shape.audit());
        arguments.add("--sdh.telemetry.journal.linger="
                + Math.max(0, shape.linger()) + "ms");
        arguments.add("--sdh.lease.jdbc.pool-size="
                + Integer.getInteger("perf.fanout.pool-size", 2));
        return arguments.toArray(String[]::new);
    }

    /**
     * The audit settings to run: {@code perf.fanout.audit}, a list of
     * {@code on}, {@code off} and {@code on:<ms>} (on, with that linger).
     *
     * @return each one's linger in milliseconds, -1 for off, in order
     */
    static List<Integer> audits()
    {
        String setting = System.getProperty("perf.fanout.audit", "");
        return Arrays.stream((setting.isBlank() ? "on,off" : setting)
                .split(","))
                .map(String::trim).filter(value -> !value.isEmpty())
                .map(value -> value.equals("on") ? 0
                        : value.equals("off") ? -1
                        : value.startsWith("on:")
                                ? Integer.parseInt(value.substring(3))
                                : fail(value)).toList();
    }

    /**
     * Refuses an audit setting.
     *
     * @param value the setting
     * @return never
     * @throws IllegalArgumentException always
     */
    static int fail(String value)
    {
        throw new IllegalArgumentException("perf.fanout.audit takes on, off"
                + " and on:<ms>, not " + value);
    }

    /**
     * A list of numbers from a setting.
     *
     * @param property   the setting
     * @param otherwise  its default, when unset or empty
     * @return the numbers, in order
     */
    static List<Integer> ints(String property, String otherwise)
    {
        String value = System.getProperty(property, "");
        if (value.isBlank())
        {
            value = otherwise;
        }
        return Arrays.stream(value.split(",")).map(String::trim)
                .filter(each -> !each.isEmpty()).map(Integer::valueOf)
                .toList();
    }

    /**
     * A point's line for the console as it finishes.
     *
     * @param point the point
     * @return the line
     */
    static String line(Point point)
    {
        double seconds = point.wallNanos() / 1e9;
        return String.format("fanout %s: %d items, %d records in %.1f s:"
                + " %.1f items/s, %.0f records/s", point.shape().name(),
                point.shape().items(), point.records(), seconds,
                point.shape().items() / seconds, point.records() / seconds);
    }

    /**
     * The results, as Markdown: a table of the points, then each point's
     * phases, per item and per record.
     *
     * @param storageDescription where the tables were
     * @return the report
     */
    static String report(String storageDescription)
    {
        StringBuilder out = new StringBuilder();
        out.append("# Split fan-out, ").append(Instant.now()).append("\n\n");
        out.append("Storage: ").append(storageDescription)
                .append(". Broker: a queue in memory. Postgres: embedded,"
                        + " on this host.\n\n");
        out.append("| rows/item | row B | conc | batch | audit | items"
                + " | records | MB | s | items/s | records/s | MB/s"
                + " | item p50 ms | p95 | max | JVM cores | PG CPU s"
                + " | PG ms/item | PG µs/rec | xacts/item | rows/item"
                + " | WAL KB/item |\n");
        out.append("|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:"
                + "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:"
                + "|---:|\n");
        for (Point point : POINTS)
        {
            Shape shape = point.shape();
            double seconds = point.wallNanos() / 1e9;
            int items = shape.items();
            Postgres pg = point.postgres();
            long rows = pg.inserted() + pg.updated() + pg.deleted();
            out.append(String.format("| %d | %d | %d | %d | %s | %d | %d"
                    + " | %.1f | %.1f | %.1f | %.0f | %.1f | %.0f | %.0f"
                    + " | %.0f | %.2f | %.1f | %.1f | %.0f | %.1f | %.1f"
                    + " | %.1f |%n",
                    shape.rows(), shape.rowBytes(), shape.concurrency(),
                    shape.batch(), shape.auditName(), items,
                    point.records(), point.bytes() / 1e6, seconds,
                    items / seconds, point.records() / seconds,
                    point.bytes() / 1e6 / seconds, point.latency().p50(),
                    point.latency().p95(), point.latency().max(),
                    point.resources().cores(), pg.cpuSeconds(),
                    pg.cpuSeconds() * 1e3 / items,
                    pg.cpuSeconds() * 1e6 / point.records(),
                    (double) pg.transactions() / items,
                    (double) rows / items,
                    pg.walBytes() / 1024.0 / items));
        }
        out.append("\nPhases: the stage's timers summed over the point;"
                + " per item and per record are the sum divided by the"
                + " items and records. Phases overlap (records holds"
                + " records_read and records_store; split holds most of"
                + " the rest; item holds all).\n");
        for (Point point : POINTS)
        {
            Shape shape = point.shape();
            out.append("\n### ").append(shape.name()).append("\n\n");
            out.append("| phase | count | mean ms | ms/item | µs/record"
                    + " |\n|---|---:|---:|---:|---:|\n");
            point.phases().forEach((phase, times) -> out.append(
                    String.format("| %s | %d | %.3f | %.3f | %.2f |%n",
                            phase, times.count(),
                            times.sum() * 1e3 / times.count(),
                            times.sum() * 1e3 / shape.items(),
                            times.sum() * 1e6 / Math.max(1,
                                    point.records()))));
            List<String> statements = STATEMENTS.getOrDefault(shape.name(),
                    List.of());
            if (!statements.isEmpty())
            {
                out.append("\nStatements (pg_stat_statements, the ")
                        .append(STATEMENTS_LISTED)
                        .append(" commonest):\n\n| calls/item | mean ms"
                                + " | statement |\n|---:|---:|---|\n");
                statements.forEach(row -> out.append(row).append('\n'));
            }
        }
        return out.toString();
    }
}
