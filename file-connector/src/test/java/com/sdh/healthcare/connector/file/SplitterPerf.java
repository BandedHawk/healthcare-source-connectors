/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.perf.Latencies;
import com.sdh.healthcare.connector.core.perf.PerfOutput;
import com.sdh.healthcare.connector.core.perf.PerfSettings;
import com.sdh.healthcare.connector.core.perf.PerfStorage;
import com.sdh.healthcare.connector.core.perf.ResourceMonitor;
import com.sdh.healthcare.connector.core.perf.StageResult;
import com.sdh.healthcare.connector.core.perf.Timeline;
import com.sdh.healthcare.connector.core.perf.Workload;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
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
 * The splitting stage's performance, and the pipeline's end to end (design
 * §13.1): the real splitter, in process, on embedded Postgres and the
 * {@code perf.storage} tables, with Camel queues standing in for the
 * brokers.
 * <p>
 * Each workload, and each intake it landed through, runs on a splitter of
 * its own. The test lands the same items the landing stage did, untimed,
 * then sends their {@code item.landed} messages with the gaps between them
 * that the landing stage's timeline recorded, the first at once, so that
 * the splitter sees the arrivals the landing node made. An item's
 * splitting latency is from its message sent to its {@code file.completed}
 * published. End to end is each item's landing latency plus its splitting
 * latency. Without a timeline, from a landing stage that didn't run, the
 * messages go at the workload's own schedule and there is no end to end.
 * <p>
 * Run with {@code ./mvnw -Pperf -pl landing-connector,file-connector -am
 * verify -Dperf.stages=landing,split}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class SplitterPerf
{

    /** Where {@code item.landed} messages arrive. */
    static final String LANDED_QUEUE = "seda:perf-landed?size=100000";

    /**
     * Where the splitter's messages go: a bounded queue that holds the
     * splitter up when full, as a slow broker would.
     */
    static final String OUT_QUEUE =
            "seda:perf-split-out?size=10000&blockWhenFull=true";

    /** The splitter's namespace each run uses, and drops after. */
    static final String NAMESPACE = "perf_split";

    /** How long a run may take, at most. */
    static final Duration RUN_LIMIT = Duration.ofMinutes(60);

    /** When each item's {@code file.completed} was published, by item. */
    static final Map<UUID, Long> COMPLETED = new ConcurrentHashMap<>();

    /** The sections stored in the current run, failed ones included. */
    static final AtomicLong SECTIONS = new AtomicLong();

    /** The reasons given to the exit hook. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** The tables' storage, for every run. */
    static PerfStorage storage;

    /** The test's files. */
    static Path root;

    /** The key files. */
    static TestKeys keys;

    /** The runs' results. */
    static final List<StageResult> RESULTS = new ArrayList<>();

    /** Notes for the report. */
    static final List<String> NOTES = new ArrayList<>();

    /** The beans the splitter gets for the test. */
    @Configuration(proxyBeanMethods = false)
    static class PerfBeans
    {

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
         * The broker stand-in: counts sections and notes when each item's
         * {@code file.completed} came, by the unencrypted headers, and
         * drops the messages.
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
                    from(OUT_QUEUE).routeId("perf-broker").process(exchange ->
                    {
                        long at = System.nanoTime();
                        String type = exchange.getIn().getHeader("type",
                                String.class);
                        if (MessageType.FILE_COMPLETED.wireName()
                                .equals(type))
                        {
                            COMPLETED.put(UUID.fromString(exchange.getIn()
                                    .getHeader("fileId", String.class)), at);
                        }
                        else if (MessageType.SECTION_STORED.wireName()
                                .equals(type)
                                || MessageType.SECTION_FAILED.wireName()
                                        .equals(type))
                        {
                            SECTIONS.incrementAndGet();
                        }
                    });
                }
            };
        }
    }

    /**
     * Starts the storage and makes the keys, if the splitting stage is to
     * run.
     *
     * @throws Exception if one can't be made
     */
    @BeforeAll
    static void start() throws Exception
    {
        Assumptions.assumeTrue(PerfSettings.runs(PerfSettings.SPLIT),
                "perf.stages doesn't include split");
        storage = PerfStorage.create(PerfSettings.storage());
        root = Files.createDirectories(PerfSettings.out().resolve("work")
                .resolve("split"));
        clear(root);
        keys = new TestKeys(Files.createDirectories(root.resolve("keys")));
    }

    /**
     * Records the results, and closes the storage.
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
            if (!RESULTS.isEmpty())
            {
                NOTES.add("Storage: " + storage.describe() + ".");
                NOTES.add("Splitting latency is from an item.landed sent to"
                        + " the item's file.completed; end to end adds the"
                        + " item's landing latency from the landing"
                        + " stage's timeline.");
                PerfOutput.record(PerfSettings.SPLIT, RESULTS, NOTES);
            }
        }
        finally
        {
            storage.close();
            clear(root);
        }
    }

    /**
     * Splits each workload, as it arrived through each of its intakes.
     *
     * @throws Exception if a run fails
     */
    @Test
    void splitting() throws Exception
    {
        for (String workload : PerfSettings.workloads())
        {
            for (String intake : Workload.intakes(workload))
            {
                run(workload, intake);
            }
        }
    }

    /**
     * One run: a splitter of its own splits a workload, fed at the
     * landing stage's announcements.
     *
     * @param workload the workload
     * @param intake   the intake it landed through
     * @throws Exception if the run fails
     */
    void run(String workload, String intake) throws Exception
    {
        Optional<List<Timeline.Entry>> recorded =
                Timeline.read(workload, intake);
        List<Timeline.Entry> timeline = recorded.orElseGet(() ->
                schedule(workload));
        if (recorded.isEmpty())
        {
            NOTES.add(workload + " / " + intake + ": no landing timeline;"
                    + " sent at the workload's schedule, no end to end.");
        }

        COMPLETED.clear();
        SECTIONS.set(0);
        FATAL.clear();
        String name = "perf-" + workload + "-" + intake;
        Path work = Files.createDirectories(root.resolve(name));
        try (ConfigurableApplicationContext splitter =
                new SpringApplicationBuilder(FileConnectorApplication.class,
                        PerfBeans.class)
                        .run(arguments(workload, name)))
        {
            // The items, landed as the landing stage landed them: untimed.
            TestLanding landing = new TestLanding(storage.catalogProperties(),
                    splitter.getBean(ConnectorRuntime.class).keys().wrap(),
                    keys, name);
            List<LandedItem> items = land(workload, landing, work);
            long bytes = items.stream().mapToLong(LandedItem::size).sum();
            Map<UUID, Integer> bySeq = new LinkedHashMap<>();
            for (int seq = 0; seq < items.size(); seq++)
            {
                bySeq.put(items.get(seq).itemId(), seq);
            }
            List<byte[]> announcements = items.stream()
                    .map(landing::announcement).toList();

            ProducerTemplate producer = splitter.getBean(CamelContext.class)
                    .createProducerTemplate();
            long[] sent = new long[items.size()];
            // The first announcement goes at once: the splitter's time
            // starts there, not at the landing run's start.
            long first = timeline.stream()
                    .mapToLong(Timeline.Entry::announced).min().orElse(0);
            ResourceMonitor monitor = ResourceMonitor.start();
            long start = System.nanoTime();
            for (Timeline.Entry entry : timeline)
            {
                awaitDue(start + entry.announced() - first);
                sent[entry.seq()] = System.nanoTime();
                producer.sendBody(LANDED_QUEUE,
                        announcements.get(entry.seq()));
            }
            long deadline = System.nanoTime() + RUN_LIMIT.toNanos();
            while (COMPLETED.size() < items.size()
                    && System.nanoTime() < deadline && FATAL.isEmpty())
            {
                Thread.sleep(5);
            }
            ResourceMonitor.Resources resources = monitor.stop();
            assertThat(FATAL).as("fatal exits").isEmpty();
            assertThat(COMPLETED.keySet()).as("file.completed messages")
                    .containsExactlyInAnyOrderElementsOf(bySeq.keySet());

            Latencies split = new Latencies();
            Latencies endToEnd = new Latencies();
            long last = start;
            long endToEndLast = 0;
            long firstAvailable = Long.MAX_VALUE;
            for (Timeline.Entry entry : timeline)
            {
                long completed = COMPLETED.get(items.get(entry.seq())
                        .itemId());
                long splitting = completed - sent[entry.seq()];
                split.add(splitting);
                endToEnd.add(entry.landing() + splitting);
                last = Math.max(last, completed);
                endToEndLast = Math.max(endToEndLast,
                        entry.announced() + splitting);
                firstAvailable = Math.min(firstAvailable, entry.available());
            }
            RESULTS.add(new StageResult(PerfSettings.SPLIT, workload, intake,
                    items.size(), bytes, SECTIONS.get(), last - start,
                    split.summary(), resources, Double.NaN));
            if (recorded.isPresent())
            {
                RESULTS.add(new StageResult(PerfSettings.END_TO_END,
                        workload, intake, items.size(), bytes,
                        SECTIONS.get(), endToEndLast - firstAvailable,
                        endToEnd.summary(), null, Double.NaN));
            }
        }
        finally
        {
            storage.dropNamespace(NAMESPACE);
            storage.dropNamespace(LandingTables.DEFAULT_NAMESPACE);
            clear(work);
            Files.deleteIfExists(work);
        }
    }

    /**
     * Lands a workload's items, in order: the bulk file, written to disk
     * first, or each small document.
     *
     * @param workload the workload
     * @param landing  lands them
     * @param work     the run's directory
     * @return the items
     * @throws Exception if one can't be landed
     */
    static List<LandedItem> land(String workload, TestLanding landing,
            Path work) throws Exception
    {
        if (Workload.bulk(workload))
        {
            Path file = work.resolve("bulk." + Workload.format(workload));
            Workload.bulkFile(workload, file);
            LandedItem item = landing.land(file, null);
            Files.delete(file);
            return List.of(item);
        }
        Workload.SmallDocuments documents = new Workload.SmallDocuments();
        List<LandedItem> items = new ArrayList<>();
        for (int seq = 0; seq < Workload.items(workload); seq++)
        {
            items.add(landing.land(documents.next(), null));
        }
        return items;
    }

    /**
     * The workload's own schedule, for when landing hasn't run: each item
     * announced when it is due, with no landing latency.
     *
     * @param workload the workload
     * @return the entries, in order
     */
    static List<Timeline.Entry> schedule(String workload)
    {
        List<Timeline.Entry> entries = new ArrayList<>();
        for (int seq = 0; seq < Workload.items(workload); seq++)
        {
            long due = Workload.due(workload, seq);
            entries.add(new Timeline.Entry(seq, -1, due, due));
        }
        return entries;
    }

    /**
     * Waits until a moment.
     *
     * @param due when, from {@link System#nanoTime()}
     * @throws InterruptedException if interrupted
     */
    static void awaitDue(long due) throws InterruptedException
    {
        long wait;
        while ((wait = due - System.nanoTime()) > 0)
        {
            Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
        }
    }

    /**
     * The splitter's settings for a run, as command-line arguments so that
     * they win over {@code application.yml}.
     *
     * @param workload the workload
     * @param intake   the intake name the items landed under
     * @return the arguments
     * @throws Exception if the recipient's fingerprint can't be made
     */
    static String[] arguments(String workload, String intake)
            throws Exception
    {
        String jdbc = TestPostgres.jdbcUrl();
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("camel.main.run-controller", "false");
        settings.put("sdh.landing.intake", intake);
        settings.put("sdh.landing.queue-uri", LANDED_QUEUE);
        settings.put("sdh.landing.idle-wait", "200ms");
        settings.put("sdh.split.format", Workload.format(workload));
        settings.put("sdh.split.limits.max-file-bytes", "64GB");
        settings.putAll(storage.applicationSettings());
        settings.put("sdh.storage.catalog-name", TestLanding.CATALOG);
        settings.put("sdh.storage.namespace", NAMESPACE);
        settings.put("sdh.lease.jdbc.url", jdbc);
        settings.put("sdh.lease.jdbc.username", "postgres");
        settings.put("sdh.lease.jdbc.password", "postgres");
        settings.put("sdh.crypto.keystore.path", keys.keystore.toString());
        settings.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
        settings.put("sdh.crypto.keys.sections-kek", TestKeys.SECTIONS);
        settings.put("sdh.crypto.keys.landed-items-kek", TestKeys.LANDED);
        settings.put("sdh.crypto.keys.signing.kid", "connector-2026");
        settings.put("sdh.crypto.keys.signing.pem",
                keys.signingPem.toString());
        settings.put("sdh.crypto.keys.signing.passphrase",
                TestKeys.SIGNING_PASSPHRASE);
        settings.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
        settings.put("sdh.crypto.keys.recipient.pem",
                keys.recipientPem.toString());
        settings.put("sdh.crypto.keys.recipient.sha256",
                KeyMaterial.fingerprint(keys.recipient.getPublic()));
        settings.put("sdh.crypto.keys.inbox.kid", TestKeys.INBOX_KID);
        settings.put("sdh.crypto.keys.inbox.pem", keys.inboxPem.toString());
        settings.put("sdh.crypto.keys.inbox.passphrase",
                TestKeys.INBOX_PASSPHRASE);
        settings.put("sdh.crypto.keys.landing-signer.kid",
                TestKeys.LANDING_SIGNER_KID);
        settings.put("sdh.crypto.keys.landing-signer.pem",
                keys.landingSignerPem.toString());
        settings.put("sdh.queue.type", "camel");
        settings.put("sdh.queue.uri", OUT_QUEUE);
        settings.put("sdh.results.enabled", "false");
        settings.put("sdh.maintenance.enabled", "false");
        return settings.entrySet().stream()
                .map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                .toArray(String[]::new);
    }

    /**
     * Deletes what is in a directory of the test's own.
     *
     * @param directory the directory
     * @throws IOException if something can't be deleted
     */
    static void clear(Path directory) throws IOException
    {
        if (!Files.exists(directory))
        {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory))
        {
            for (Path path : paths.sorted(Comparator.reverseOrder())
                    .filter(path -> !path.equals(directory)).toList())
            {
                Files.delete(path);
            }
        }
    }
}
