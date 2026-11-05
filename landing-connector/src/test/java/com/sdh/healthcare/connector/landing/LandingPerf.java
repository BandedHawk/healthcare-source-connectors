/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.perf.Latencies;
import com.sdh.healthcare.connector.core.perf.PerfOutput;
import com.sdh.healthcare.connector.core.perf.PerfSettings;
import com.sdh.healthcare.connector.core.perf.PerfStorage;
import com.sdh.healthcare.connector.core.perf.ResourceMonitor;
import com.sdh.healthcare.connector.core.perf.StageResult;
import com.sdh.healthcare.connector.core.perf.Timeline;
import com.sdh.healthcare.connector.core.perf.Workload;
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.landing.config.SourceSettings;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
 * The landing stage's performance (design §13.1): the real landing node,
 * in process, on embedded Postgres and the {@code perf.storage} tables,
 * with a Camel route standing in for the broker.
 * <p>
 * Each workload runs on a node of its own: a bulk file through a directory
 * intake, and many small C-CDA documents through each of
 * {@code perf.intakes}, a directory or HTTP push with mTLS, arriving at
 * {@code perf.rate} a second. An item's latency is from when it became
 * available (its {@code .done} marker written, or its push sent) to when
 * its {@code item.landed} message reached the broker. Each run's timeline
 * is kept for the splitter stage to replay.
 * <p>
 * Run with {@code ./mvnw -Pperf -pl landing-connector -am verify
 * -Dperf.stages=landing}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class LandingPerf
{

    /**
     * The broker stand-in's endpoint: a queue, so that the node's send
     * completes on the stand-in's thread, as a broker client's does on its
     * own. A {@code direct:} route would run behind the node's route on
     * the same thread, and never before the node stopped waiting.
     */
    static final String BROKER = "seda:perf-landed?size=100000";

    /** The subject the HTTP client's certificate is issued to. */
    static final String CLIENT = "CN=perf-producer,O=Example";

    /** The landing namespace each run uses, and drops after. */
    static final String NAMESPACE = "perf_landing";

    /** The most pushes in flight at once. */
    static final int IN_FLIGHT = 32;

    /** How long a run may take, at most. */
    static final Duration RUN_LIMIT = Duration.ofMinutes(30);

    /**
     * An {@code item.landed} message, as the broker got it.
     *
     * @param at   when, from {@link System#nanoTime()}
     * @param body the sealed message
     */
    record Announcement(long at, String body)
    {
    }

    /** The messages the broker stand-in has got in the current run. */
    static final List<Announcement> ANNOUNCED = new CopyOnWriteArrayList<>();

    /** The reasons given to the exit hook. */
    static final List<String> FATAL = new CopyOnWriteArrayList<>();

    /** The tables' storage, for every run. */
    static PerfStorage storage;

    /** The test's files. */
    static Path root;

    /** The key files. */
    static LandingTestKeys keys;

    /** The certificates, for HTTP push. */
    static PushTestPki pki;

    /** The runs' results. */
    static final List<StageResult> RESULTS = new ArrayList<>();

    /** The beans the node gets for the test. */
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
         * The broker stand-in: notes when each {@code item.landed} came,
         * and keeps it, sealed, to be opened after the run.
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
                    from(BROKER).routeId("perf-broker").process(exchange ->
                    {
                        long at = System.nanoTime();
                        if ("item.landed".equals(exchange.getIn()
                                .getHeader("type", String.class)))
                        {
                            ANNOUNCED.add(new Announcement(at, exchange
                                    .getIn().getBody(String.class)));
                        }
                    });
                }
            };
        }
    }

    /**
     * Starts the storage and makes the keys and certificates, if the
     * landing stage is to run.
     *
     * @throws Exception if one can't be made
     */
    @BeforeAll
    static void start() throws Exception
    {
        Assumptions.assumeTrue(PerfSettings.runs(PerfSettings.LANDING),
                "perf.stages doesn't include landing");
        storage = PerfStorage.create(PerfSettings.storage());
        root = Files.createDirectories(PerfSettings.out().resolve("work")
                .resolve("landing"));
        clear(root);
        keys = new LandingTestKeys(
                Files.createDirectories(root.resolve("keys")));
        pki = new PushTestPki(root.resolve("pki"), "perf-ca");
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
                PerfOutput.record(PerfSettings.LANDING, RESULTS, List.of(
                        "Storage: " + storage.describe() + ".",
                        "Small documents: " + PerfSettings.items()
                                + " at " + PerfSettings.rate() + "/s"
                                + " (0: all at once); latency is from an"
                                + " item available to its item.landed at"
                                + " the broker."));
            }
        }
        finally
        {
            storage.close();
            clear(root);
        }
    }

    /**
     * Lands each workload through each of its intakes.
     *
     * @throws Exception if a run fails
     */
    @Test
    void landing() throws Exception
    {
        for (String workload : PerfSettings.workloads())
        {
            for (String intake : Workload.intakes(workload))
            {
                RESULTS.add(run(workload, intake));
            }
        }
    }

    /**
     * One run: a node of its own lands a workload through an intake.
     *
     * @param workload the workload
     * @param intake   the intake
     * @return the result
     * @throws Exception if the run fails
     */
    StageResult run(String workload, String intake) throws Exception
    {
        Path work = Files.createDirectories(root.resolve(workload + "-"
                + intake));
        Path drop = Files.createDirectories(work.resolve("drop"));
        Path staging = Files.createDirectories(work.resolve("staging"));
        List<Path> items = stage(workload, staging);
        Map<Long, Integer> bySize = new HashMap<>();
        long bytes = 0;
        for (int seq = 0; seq < items.size(); seq++)
        {
            long size = Files.size(items.get(seq));
            bytes += size;
            assertThat(bySize.put(size, seq)).as("unique sizes").isNull();
        }

        ANNOUNCED.clear();
        FATAL.clear();
        boolean http = intake.equals(PerfSettings.HTTP);
        try (ConfigurableApplicationContext node = new SpringApplicationBuilder(
                LandingConnectorApplication.class, PerfBeans.class)
                .run(arguments(workload, intake, drop)))
        {
            long[] available = new long[items.size()];
            ResourceMonitor monitor = ResourceMonitor.start();
            long start = System.nanoTime();
            if (http)
            {
                int port = Integer.parseInt(node.getEnvironment()
                        .getRequiredProperty("local.server.port"));
                push(workload, items, port, start, available);
            }
            else
            {
                drop(workload, items, drop, start, available);
            }
            long deadline = System.nanoTime() + RUN_LIMIT.toNanos();
            while (ANNOUNCED.size() < items.size()
                    && System.nanoTime() < deadline && FATAL.isEmpty())
            {
                Thread.sleep(5);
            }
            ResourceMonitor.Resources resources = monitor.stop();
            assertThat(FATAL).as("fatal exits").isEmpty();
            assertThat(ANNOUNCED).as("item.landed messages")
                    .hasSize(items.size());

            MessageOpener opener = new MessageOpener(
                    Map.of("downstream-2026", keys.recipient.getPrivate()),
                    Map.of("landing-2026",
                            (ECPublicKey) keys.signing.getPublic()));
            Latencies latencies = new Latencies();
            List<Timeline.Entry> timeline = new ArrayList<>();
            long last = start;
            for (Announcement announcement : ANNOUNCED)
            {
                ConnectorMessage.ItemLanded landed =
                        (ConnectorMessage.ItemLanded) opener.open(
                                announcement.body().getBytes(UTF_8));
                Integer seq = bySize.get(landed.size());
                assertThat(seq).as("a staged item's size").isNotNull();
                timeline.add(new Timeline.Entry(seq, landed.size(),
                        available[seq] - start, announcement.at() - start));
                latencies.add(announcement.at() - available[seq]);
                last = Math.max(last, announcement.at());
            }
            Timeline.write(workload, intake, timeline);
            return new StageResult(PerfSettings.LANDING, workload, intake,
                    items.size(), bytes, -1, last - start,
                    latencies.summary(), resources, Double.NaN);
        }
        finally
        {
            storage.dropNamespace(NAMESPACE);
            clear(work);
            Files.deleteIfExists(work);
        }
    }

    /**
     * Writes a workload's items to the staging directory, outside the
     * timing: the bulk file, or each small document.
     *
     * @param workload the workload
     * @param staging  the staging directory
     * @return the items, in order
     * @throws IOException if one can't be written
     */
    static List<Path> stage(String workload, Path staging) throws IOException
    {
        if (Workload.bulk(workload))
        {
            Path file = staging.resolve("bulk." + Workload.format(workload));
            Workload.bulkFile(workload, file);
            return List.of(file);
        }
        Workload.SmallDocuments documents = new Workload.SmallDocuments();
        List<Path> items = new ArrayList<>();
        for (int seq = 0; seq < Workload.items(workload); seq++)
        {
            items.add(Files.write(staging.resolve("doc-" + seq + ".xml"),
                    documents.next()));
        }
        return items;
    }

    /**
     * Makes items available to a directory intake, each when it is due:
     * moved into the directory, then its marker written.
     *
     * @param workload  the workload
     * @param items     the staged items
     * @param drop      the intake's directory
     * @param start     the run's start
     * @param available where to note when each became available
     * @throws Exception if one can't be moved
     */
    static void drop(String workload, List<Path> items, Path drop,
            long start, long[] available) throws Exception
    {
        for (int seq = 0; seq < items.size(); seq++)
        {
            awaitDue(start + Workload.due(workload, seq));
            Path item = drop.resolve(items.get(seq).getFileName());
            available[seq] = System.nanoTime();
            Files.move(items.get(seq), item, StandardCopyOption.ATOMIC_MOVE);
            Files.createFile(drop.resolve(item.getFileName()
                    + SourceSettings.MARKER_SUFFIX));
        }
    }

    /**
     * Pushes items to an HTTP intake, each when it is due, with at most
     * {@link #IN_FLIGHT} at once. A push the node is too busy for (503) is
     * sent again; it wasn't landed.
     *
     * @param workload  the workload
     * @param items     the staged items
     * @param port      the node's port
     * @param start     the run's start
     * @param available where to note when each became available
     * @throws Exception if a push fails
     */
    static void push(String workload, List<Path> items, int port, long start,
            long[] available) throws Exception
    {
        HttpClient client = HttpClient.newBuilder()
                .sslContext(pki.clientContext(pki.issue(CLIENT, false)))
                .connectTimeout(Duration.ofSeconds(5)).build();
        URI uri = URI.create("https://127.0.0.1:" + port + "/intakes/"
                + intakeName(workload, PerfSettings.HTTP) + "/items");
        Semaphore slots = new Semaphore(IN_FLIGHT);
        List<CompletableFuture<Integer>> pushes = new ArrayList<>();
        for (int seq = 0; seq < items.size(); seq++)
        {
            awaitDue(start + Workload.due(workload, seq));
            slots.acquire();
            available[seq] = System.nanoTime();
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofMinutes(2))
                    .header("Content-Type", "application/octet-stream")
                    .POST(HttpRequest.BodyPublishers.ofFile(items.get(seq)))
                    .build();
            pushes.add(send(client, request)
                    .whenComplete((status, failure) -> slots.release()));
        }
        for (CompletableFuture<Integer> push : pushes)
        {
            assertThat(push.join()).as("push status").isEqualTo(202);
        }
    }

    /**
     * Sends a push, again after a moment while the node is busy.
     *
     * @param client  the client
     * @param request the push
     * @return its final status
     */
    static CompletableFuture<Integer> send(HttpClient client,
            HttpRequest request)
    {
        return client.sendAsync(request, HttpResponse.BodyHandlers.discarding())
                .thenCompose(response -> response.statusCode() == 503
                        ? CompletableFuture.supplyAsync(() -> null,
                                CompletableFuture.delayedExecutor(50,
                                        TimeUnit.MILLISECONDS))
                                .thenCompose(ignored -> send(client, request))
                        : CompletableFuture.completedFuture(
                                response.statusCode()));
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
     * The node's settings for a run, as command-line arguments so that
     * they win over {@code application.yml}.
     *
     * @param workload the workload
     * @param intake   the intake
     * @param drop     the directory intake's directory
     * @return the arguments
     * @throws Exception if the recipient's fingerprint can't be made
     */
    static String[] arguments(String workload, String intake, Path drop)
            throws Exception
    {
        String jdbc = TestPostgres.jdbcUrl();
        Map<String, String> settings = new LinkedHashMap<>();
        settings.put("camel.main.run-controller", "false");
        settings.put("sdh.landing.intake", intakeName(workload, intake));
        if (intake.equals(PerfSettings.HTTP))
        {
            settings.put("sdh.landing.source.type", "http");
            settings.put("sdh.landing.http.port", "0");
            settings.put("sdh.landing.http.tls.certificate",
                    pki.serverCertificate.toString());
            settings.put("sdh.landing.http.tls.private-key",
                    pki.serverKey.toString());
            settings.put("sdh.landing.http.auth", "mtls");
            settings.put("sdh.landing.http.mtls.trusted-cas",
                    pki.caCertificate.toString());
            settings.put("sdh.landing.http.mtls.subjects[0]", CLIENT);
            settings.put("sdh.landing.http.max-item-bytes", "16MB");
        }
        else
        {
            settings.put("sdh.landing.source.path", drop.toString());
            settings.put("sdh.landing.source.access", "read-write");
            settings.put("sdh.landing.source.ready", "marker");
            settings.put("sdh.landing.source.poll-interval", "20ms");
        }
        settings.putAll(storage.applicationSettings());
        settings.put("sdh.storage.namespace", NAMESPACE);
        settings.put("sdh.lease.jdbc.url", jdbc);
        settings.put("sdh.lease.jdbc.username", "postgres");
        settings.put("sdh.lease.jdbc.password", "postgres");
        settings.put("sdh.crypto.keystore.path", keys.keystore.toString());
        settings.put("sdh.crypto.keystore.password",
                LandingTestKeys.KEYSTORE_PASSWORD);
        settings.put("sdh.crypto.keys.landed-items-kek",
                LandingTestKeys.LANDED_ITEMS);
        settings.put("sdh.crypto.keys.signing.kid", "landing-2026");
        settings.put("sdh.crypto.keys.signing.pem",
                keys.signingPem.toString());
        settings.put("sdh.crypto.keys.signing.passphrase",
                LandingTestKeys.SIGNING_PASSPHRASE);
        settings.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
        settings.put("sdh.crypto.keys.recipient.pem",
                keys.recipientPem.toString());
        settings.put("sdh.crypto.keys.recipient.sha256",
                KeyMaterial.fingerprint(keys.recipient.getPublic()));
        settings.put("sdh.queue.type", "camel");
        settings.put("sdh.queue.uri", BROKER);
        settings.put("sdh.maintenance.enabled", "false");
        return settings.entrySet().stream()
                .map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                .toArray(String[]::new);
    }

    /**
     * A run's intake name, its own so that leases and ledgers don't mix.
     *
     * @param workload the workload
     * @param intake   the intake
     * @return the name
     */
    static String intakeName(String workload, String intake)
    {
        return "perf-" + workload + "-" + intake;
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
