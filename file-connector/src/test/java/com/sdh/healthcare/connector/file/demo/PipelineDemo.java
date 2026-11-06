/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.demo;

import static java.nio.charset.StandardCharsets.UTF_8;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.demo.DemoFiles.DemoFile;
import com.sdh.healthcare.connector.file.demo.DemoFiles.Push;
import com.sdh.healthcare.connector.file.demo.OtlpReceiver.AuditEvent;
import com.sdh.healthcare.connector.reader.QueueReader;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.net.ssl.SSLContext;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * A demonstration of the consolidated pipeline (design §15.3): the landing
 * node and the splitter, each run as a process of its own from the JAR the
 * build made, connected by a broker as they are deployed. Two pipelines
 * run one after the other, each an intake with its own landing node and
 * splitter:
 * <ol>
 *   <li>{@code demo-files}: C-CDA files dropped one by one into a watched
 *       directory, each followed by its {@code .done} marker. Among them
 *       are the three ways a file fails to split, a file sent twice, and a
 *       corrected file that names the one it replaces;
 *   <li>{@code demo-push}: documents pushed over HTTPS with mTLS, and the
 *       pushes the landing node refuses: an unlisted client, one too
 *       large, and one replacing an item that doesn't exist.
 * </ol>
 * The console shows each item's way through: the demo's own commentary
 * ({@code [demo]}), each app's log ({@code [landing]}, {@code [splitter]})
 * with its errors and warnings, and the telemetry both apps export over
 * OTLP to the demo's {@link OtlpReceiver}: audit events
 * ({@code [otel log]}), traces ({@code [otel trace]}) and metrics
 * ({@code [otel metrics]}). The demo follows each item by the audit
 * events: landed and announced by the landing node, then split, and
 * completed or failed by the splitter. At the end of each pipeline a
 * downstream consumer reads the converted documents back.
 * <p>
 * Everything is kept in {@code file-connector/target/demo/}: the apps'
 * logs ({@code logs/}), their telemetry as OTLP JSON
 * ({@code telemetry/<service>/}), a report that follows each item through
 * the pipeline by its telemetry ({@code telemetry/report.md}), and the
 * converted documents ({@code output/}).
 * <p>
 * Run with {@code -Pdemo} ({@code ./mvnw -Pdemo -pl
 * landing-connector,file-connector -am verify}), or from NetBeans (Run
 * Maven &gt; Demo). Never part of a normal build or the integration tests.
 * Settings: {@code demo.stack} {@code local} (default: no Docker) or
 * {@code vm} (Postgres, SeaweedFS and Kafka in the minikube VM);
 * {@code demo.pause} between items (default 3s);
 * {@code demo.metric-interval} (default 15s).
 * <p>
 * The samples are synthetic or de-identified; none contains PHI. The apps'
 * own output stays PHI-free either way: their telemetry goes through the
 * same filters as in production.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PipelineDemo
{

    /** The demo's working directory. */
    static final Path WORK = Path.of("target", "demo").toAbsolutePath();

    /** Where the converted documents are written. */
    static final Path OUTPUT = WORK.resolve("output");

    /** The pause between items. */
    static final Duration PAUSE = duration("demo.pause", "3s");

    /** How often the apps export metrics. */
    static final Duration METRIC_INTERVAL =
            duration("demo.metric-interval", "15s");

    /** How long one item may take through the pipeline. */
    static final Duration ITEM_LIMIT = Duration.ofMinutes(3);

    /** The directory pipeline. */
    static final String FILES = "demo-files";

    /** The HTTP push pipeline. */
    static final String PUSH = "demo-push";

    /** The landing node's service name, in its telemetry. */
    static final String LANDING = "landing-connector";

    /** The splitter's service name, in its telemetry. */
    static final String SPLITTER = "file-connector";

    /** The producer the HTTP intake lists. */
    static final String PRODUCER = "CN=claims-producer,O=Example";

    /** A client with a certificate from the same CA, but not listed. */
    static final String STRANGER = "CN=stranger,O=Example";

    /** Pretty-prints the converted JSON, and reads push receipts. */
    static final ObjectMapper JSON = new ObjectMapper();

    /**
     * One item and what became of it.
     *
     * @param pipeline     the pipeline it went through
     * @param file         the item, and what was expected
     * @param itemId       its item ID; null if it wasn't landed
     * @param landing      what the landing node did
     * @param status       the splitter's final status, or the HTTP
     *                     status of a refused push
     * @param records      the records it stored
     * @param failed       how many of those weren't converted
     * @param committedSeq the last committed record of a failed item;
     *                     -1 if none
     * @param millis       how long it took, from delivered to finished
     */
    record Outcome(String pipeline, DemoFile file, UUID itemId,
            String landing, String status, long records, long failed,
            long committedSeq, long millis)
    {

        /**
         * Whether it ended as the demo expected.
         *
         * @return true if it did
         */
        boolean asExpected()
        {
            return status.equals(file.status())
                    && (!status.equals("COMPLETED")
                            || records == file.records()
                                    && failed == file.failedRecords());
        }

        /**
         * The distinct messages a downstream consumer gets for it:
         * file.started, each committed record, and file.completed if it
         * completed. A record an earlier item already sent has the same
         * message ID, so it doesn't count again: Artemis drops it by
         * {@code _AMQ_DUPL_ID}, and a consumer would drop it anyway.
         *
         * @return how many
         */
        int messages()
        {
            if (itemId == null)
            {
                return 0;
            }
            return status.equals("COMPLETED")
                    ? (int) records - file.repeats() + 2
                    : 1 + (int) Math.max(0, committedSeq + 1);
        }
    }

    /** The key files. */
    TestKeys keys;

    /** What the apps run against. */
    DemoStack stack;

    /** Receives the apps' telemetry. */
    OtlpReceiver otlp;

    /** The class path extras for the apps; null if they need none. */
    Path extras;

    /** Every item's outcome, in order. */
    final List<Outcome> outcomes = new ArrayList<>();

    /**
     * Runs both pipelines, then the summary and the telemetry report.
     *
     * @throws Exception if the demo fails
     */
    @Test
    void demo() throws Exception
    {
        quietLogging();
        deleteTree(WORK);
        Files.createDirectories(OUTPUT);
        keys = new TestKeys(Files.createDirectories(WORK.resolve("keys")));
        banner("Healthcare source connectors: pipeline demo");
        say("Two pipelines, one after the other. Each is an intake with its"
                + " own landing node and splitter, run as separate processes"
                + " from their JARs and joined by a broker:");
        say("  " + FILES + ": files dropped into a watched directory");
        say("  " + PUSH + ": documents pushed over HTTPS with mTLS");
        say("Console tags: [demo] this commentary; [landing] and [splitter]"
                + " each app's log; [otel log] audit events, [otel trace]"
                + " traces and [otel metrics] metrics (every "
                + METRIC_INTERVAL.toSeconds() + " s), as the apps exported"
                + " them over OTLP.");
        try (DemoStack running = DemoStack.create(
                System.getProperty("demo.stack", "local"), WORK);
                OtlpReceiver receiver =
                        new OtlpReceiver(WORK.resolve("telemetry")))
        {
            stack = running;
            otlp = receiver;
            say("Running against: " + stack.describe());
            say("Telemetry received at " + otlp.endpoint());
            extras = stack.localDisk() ? extras() : null;
            filesPipeline();
            pushPipeline();
            summary();
            Path report = TelemetryReport.write(otlp, outcomes,
                    otlp.directory().resolve("report.md"));
            banner("Kept for later review");
            say("Each item through the pipeline, from its telemetry: "
                    + report);
            say("Telemetry as OTLP JSON: " + otlp.directory() + "/<service>/"
                    + "{traces,logs,metrics}.jsonl");
            say("The apps' logs: " + WORK.resolve("logs"));
            say("Converted documents: " + OUTPUT);
        }
        banner("Demo finished");
    }

    // ---- the directory pipeline ----

    /**
     * The directory pipeline: each file is dropped into the intake
     * directory with its marker, landed, announced, and split.
     *
     * @throws Exception if the pipeline fails
     */
    void filesPipeline() throws Exception
    {
        List<DemoFile> files = DemoFiles.build();
        Path base = Files.createDirectories(WORK.resolve(FILES));
        Path intake = Files.createDirectories(base.resolve("intake"));
        Path landed = Files.createDirectories(base.resolve("landed"));
        Path outbox = Files.createDirectories(base.resolve("outbox"));
        banner("Pipeline 1 of 2: " + FILES + " (a watched directory)");
        say("Intake directory: " + intake);
        say("Files to deliver, one at a time:");
        for (DemoFile file : files)
        {
            say(String.format(Locale.ROOT, "  %-34s %,9d bytes  %s",
                    file.name(), file.content().length, file.expected()));
        }
        Map<String, String> landing = new LinkedHashMap<>();
        landing.put("sdh.landing.source.path", intake.toString());
        landing.put("sdh.landing.source.access", "read-write");
        landing.put("sdh.landing.source.after", "move");
        landing.put("sdh.landing.source.move-to", landed.toString());
        landing.put("sdh.landing.source.ready", "marker");
        landing.put("sdh.landing.source.poll-interval", "200ms");
        try (DemoApp splitter = startSplitter(FILES);
                DemoApp node = startLanding(FILES, landing))
        {
            Map<String, UUID> items = new LinkedHashMap<>();
            int number = 0;
            for (DemoFile file : files)
            {
                banner(String.format(Locale.ROOT, "%s, file %d of %d: %s",
                        FILES, ++number, files.size(), file.name()));
                say("Expected: " + file.expected());
                int from = otlp.eventCount();
                long start = System.currentTimeMillis();
                Path staged = Files.write(outbox.resolve(file.name()),
                        file.content());
                if (file.replaces() != null)
                {
                    UUID earlier = items.get(file.replaces());
                    Files.writeString(intake.resolve(file.name()
                            + ".sdh-replaces"), earlier.toString());
                    say("Wrote " + file.name() + ".sdh-replaces naming "
                            + file.replaces() + "'s item " + earlier);
                }
                Files.move(staged, intake.resolve(file.name()),
                        StandardCopyOption.ATOMIC_MOVE);
                Files.createFile(intake.resolve(file.name() + ".done"));
                say("Dropped " + file.name() + " ("
                        + file.content().length + " bytes) into the intake"
                        + " directory, then its .done marker");
                AuditEvent itemLanded = otlp.await(LANDING, "item.landed",
                        event -> String.valueOf(file.content().length)
                                .equals(event.get("bytes")),
                        from, ITEM_LIMIT);
                UUID itemId = UUID.fromString(itemLanded.get("item_id"));
                items.put(file.name(), itemId);
                Outcome outcome = follow(FILES, file, itemId, from, start,
                        landingResult(FILES, file, itemId, from, items));
                outcomes.add(outcome);
                say("The landing node moved it out of the intake: landed/"
                        + " now holds " + count(landed) + " file(s)");
                Thread.sleep(PAUSE.toMillis());
            }
            readBack(FILES);
        }
    }

    /**
     * What the landing node did with an item beyond landing it: warned of
     * a duplicate, or linked a resubmission, as the demo expected.
     *
     * @param pipeline the pipeline
     * @param file     the item
     * @param itemId   its ID
     * @param from     where its events start
     * @param items    the items so far, by file name
     * @return a short description
     * @throws Exception if an expected event doesn't come, or a link
     *                   isn't recorded
     */
    String landingResult(String pipeline, DemoFile file, UUID itemId,
            int from, Map<String, UUID> items) throws Exception
    {
        if (file.duplicateOf() != null)
        {
            AuditEvent duplicate = otlp.await(LANDING,
                    "item.duplicate_content", event -> true, from,
                    ITEM_LIMIT);
            say("The landing node warned: same content as "
                    + file.duplicateOf() + "'s item "
                    + items.get(file.duplicateOf()) + " ("
                    + OtlpReceiver.text(duplicate.attributes())
                    + "); landed anyway, as an item of its own");
            return "landed, duplicate content";
        }
        if (file.replaces() != null)
        {
            UUID earlier = items.get(file.replaces());
            UUID recorded = replaces(pipeline, itemId);
            say("landed_items records item " + itemId + " as replacing "
                    + recorded + (earlier.equals(recorded)
                            ? " (" + file.replaces() + "): linked, and "
                                    + file.replaces() + "'s item is still"
                                    + " there"
                            : ", NOT " + earlier));
            return earlier.equals(recorded) ? "landed, linked to "
                    + file.replaces() : "landed, link WRONG";
        }
        return "landed";
    }

    // ---- the HTTP push pipeline ----

    /**
     * The HTTP push pipeline: each document is pushed over HTTPS with
     * mTLS; those the landing node accepts are landed, announced and
     * split.
     *
     * @throws Exception if the pipeline fails
     */
    void pushPipeline() throws Exception
    {
        List<Push> pushes = DemoFiles.pushes();
        DemoPki pki = new DemoPki(WORK.resolve(PUSH).resolve("pki"));
        banner("Pipeline 2 of 2: " + PUSH + " (HTTP push with mTLS)");
        say("The node trusts the demo CA's client certificates, and lists"
                + " one subject: " + PRODUCER);
        say("Pushes, one at a time:");
        for (Push push : pushes)
        {
            say(String.format(Locale.ROOT, "  %-34s %,9d bytes  %s",
                    push.file().name(), push.file().content().length,
                    push.file().expected()));
        }
        Map<String, String> landing = new LinkedHashMap<>();
        landing.put("sdh.landing.source.type", "http");
        landing.put("sdh.landing.http.port", "0");
        landing.put("sdh.landing.http.tls.certificate",
                pki.serverCertificate.toString());
        landing.put("sdh.landing.http.tls.private-key",
                pki.serverKey.toString());
        landing.put("sdh.landing.http.auth", "mtls");
        landing.put("sdh.landing.http.mtls.trusted-cas",
                pki.caCertificate.toString());
        landing.put("sdh.landing.http.mtls.subjects[0]", PRODUCER);
        landing.put("sdh.landing.http.max-item-bytes",
                Integer.toString(DemoFiles.MAX_PUSH_BYTES));
        try (DemoApp splitter = startSplitter(PUSH);
                DemoApp node = startLanding(PUSH, landing))
        {
            Matcher port = Pattern.compile("started on port (\\d+)")
                    .matcher(node.await("started on port \\d+",
                            Duration.ofSeconds(30)));
            if (!port.find())
            {
                throw new IllegalStateException("No HTTP port in the log");
            }
            URI uri = URI.create("https://127.0.0.1:" + port.group(1)
                    + "/intakes/" + PUSH + "/items");
            say("Pushing to " + uri);
            SSLContext producer = pki.clientContext(pki.issue(PRODUCER,
                    false));
            SSLContext stranger = pki.clientContext(pki.issue(STRANGER,
                    false));
            Map<String, UUID> items = new LinkedHashMap<>();
            int number = 0;
            for (Push push : pushes)
            {
                DemoFile file = push.file();
                banner(String.format(Locale.ROOT, "%s, push %d of %d: %s",
                        PUSH, ++number, pushes.size(), file.name()));
                say("Expected: " + file.expected());
                int from = otlp.eventCount();
                long start = System.currentTimeMillis();
                HttpRequest.Builder request = HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofSeconds(60))
                        .header("Content-Type", "application/octet-stream")
                        .header("X-Correlation-Id", "demo-push-" + number);
                if (push.replaces() != null)
                {
                    String replaced = push.replaces().equals("unknown")
                            ? UUID.randomUUID().toString()
                            : items.get(push.replaces()).toString();
                    request.header("Replaces", replaced);
                    say("Replaces: " + replaced);
                }
                HttpResponse<String> response = HttpClient.newBuilder()
                        .sslContext(push.listed() ? producer : stranger)
                        .connectTimeout(Duration.ofSeconds(5)).build()
                        .send(request.POST(HttpRequest.BodyPublishers
                                .ofByteArray(file.content())).build(),
                                HttpResponse.BodyHandlers.ofString());
                say("Pushed " + file.name() + " as "
                        + (push.listed() ? PRODUCER : STRANGER) + ": HTTP "
                        + response.statusCode() + " " + response.body());
                if (response.statusCode() != 202)
                {
                    Outcome outcome = new Outcome(PUSH, file, null,
                            "refused",
                            Integer.toString(response.statusCode()), 0, 0,
                            -1, System.currentTimeMillis() - start);
                    outcomes.add(outcome);
                    say("Result: " + file.name() + " refused with HTTP "
                            + response.statusCode() + "; nothing landed"
                            + (outcome.asExpected() ? ""
                                    : "  (NOT AS EXPECTED)"));
                    Thread.sleep(PAUSE.toMillis());
                    continue;
                }
                UUID itemId = UUID.fromString(JSON.readTree(response.body())
                        .path("itemId").asText());
                items.put(file.name(), itemId);
                otlp.await(LANDING, "item.landed", event ->
                        itemId.toString().equals(event.get("item_id")), from,
                        ITEM_LIMIT);
                outcomes.add(follow(PUSH, file, itemId, from, start,
                        landingResult(PUSH, file, itemId, from, items)));
                Thread.sleep(PAUSE.toMillis());
            }
            readBack(PUSH);
        }
    }

    // ---- following an item ----

    /**
     * Follows a landed item through the rest of the pipeline: its
     * announcement, then the splitter's outcome.
     *
     * @param pipeline the pipeline
     * @param file     the item
     * @param itemId   its ID
     * @param from     where its events start
     * @param start    when it was delivered
     * @param landing  what the landing node did
     * @return what became of it
     * @throws Exception if it isn't finished in time
     */
    Outcome follow(String pipeline, DemoFile file, UUID itemId, int from,
            long start, String landing) throws Exception
    {
        String id = itemId.toString();
        otlp.await(LANDING, "item.announced",
                event -> id.equals(event.get("item_id")), from, ITEM_LIMIT);
        say("Landed as item " + id + " and announced to the splitter");
        AuditEvent done = otlp.await(SPLITTER,
                Set.of("item.completed", "item.failed"),
                event -> id.equals(event.get("item_id")), from, ITEM_LIMIT);
        long millis = System.currentTimeMillis() - start;
        long failed = otlp.events().stream()
                .filter(event -> event.service().equals(SPLITTER)
                        && event.name().equals("record.failed")
                        && id.equals(event.get("item_id")))
                .count();
        Outcome outcome;
        String detail;
        if (done.name().equals("item.completed"))
        {
            long records = Long.parseLong(done.get("count"));
            outcome = new Outcome(pipeline, file, itemId, landing,
                    "COMPLETED", records, failed, records - 1, millis);
            detail = records + " record(s), " + failed
                    + " stored unconverted";
        }
        else
        {
            long seq = Long.parseLong(done.get("seq"));
            outcome = new Outcome(pipeline, file, itemId, landing, "FAILED",
                    0, failed, seq, millis);
            detail = done.get("error_code") + " at byte "
                    + done.get("offset") + " in stage " + done.get("stage")
                    + "; " + (seq + 1) + " record(s) stored before it;"
                    + " the original stays in landing";
        }
        say(String.format(Locale.ROOT,
                "Result: %s is item %s: %s, %s (%s) after %.1f s%s",
                file.name(), id, landing, outcome.status(), detail,
                millis / 1000.0, outcome.asExpected() ? ""
                        : "  (NOT AS EXPECTED)"));
        return outcome;
    }

    /**
     * The item a landed item replaces, as {@code landed_items} records it.
     *
     * @param pipeline the pipeline
     * @param itemId   the item
     * @return the item it replaces; null if none
     * @throws IOException if the table can't be read
     */
    UUID replaces(String pipeline, UUID itemId) throws IOException
    {
        Catalog catalog = stack.newCatalogClient();
        Table table = LandingTables.landedItems(catalog,
                landingNamespace(pipeline));
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table)
                .where(Expressions.equal(LandingTables.ITEM_ID,
                        itemId.toString()))
                .select(LandingTables.ITEM_ID, LandingTables.REPLACES_ITEM_ID)
                .build())
        {
            for (Record row : rows)
            {
                Object replaced = row.getField(LandingTables.REPLACES_ITEM_ID);
                if (replaced != null)
                {
                    return UUID.fromString(replaced.toString());
                }
            }
        }
        return null;
    }

    // ---- the apps ----

    /**
     * Starts a pipeline's splitter and waits until it is ready.
     *
     * @param pipeline the pipeline
     * @return the running app
     * @throws Exception if it can't start
     */
    DemoApp startSplitter(String pipeline) throws Exception
    {
        Map<String, String> settings = common(pipeline);
        settings.put("sdh.landing.intake", pipeline);
        settings.put("sdh.landing.idle-wait", "200ms");
        settings.put("sdh.landing.storage.namespace",
                landingNamespace(pipeline));
        settings.put("sdh.split.format", "ccda");
        settings.put("sdh.storage.namespace", pipeline.replace('-', '_'));
        // A commit per document, so each one is stored (and announced) as
        // soon as it's split: a file that fails part-way keeps what came
        // before the failure.
        settings.put("sdh.storage.commit.max-records", "1");
        settings.put("sdh.crypto.keys.records-kek", TestKeys.RECORDS);
        settings.put("sdh.crypto.keys.signing.kid", "connector-2026");
        settings.put("sdh.crypto.keys.signing.pem",
                keys.signingPem.toString());
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
        settings.putAll(stack.splitterQueues(pipeline));
        say("Starting the " + pipeline + " splitter ("
                + DemoApp.jar("file-connector").getFileName() + ")");
        DemoApp app = new DemoApp("splitter", DemoApp.jar("file-connector"),
                extras, settings, WORK.resolve("logs")
                        .resolve(pipeline + "-splitter.log"));
        app.await("Started FileConnectorApplication", Duration.ofMinutes(2));
        return app;
    }

    /**
     * Starts a pipeline's landing node and waits until it is ready.
     *
     * @param pipeline the pipeline
     * @param source   its intake's settings
     * @return the running app
     * @throws Exception if it can't start
     */
    DemoApp startLanding(String pipeline, Map<String, String> source)
            throws Exception
    {
        Map<String, String> settings = common(pipeline);
        settings.put("sdh.connector-id", "landing-" + pipeline);
        settings.put("sdh.landing.intake", pipeline);
        settings.putAll(source);
        settings.put("sdh.storage.namespace", landingNamespace(pipeline));
        settings.put("sdh.crypto.keys.signing.kid",
                TestKeys.LANDING_SIGNER_KID);
        settings.put("sdh.crypto.keys.signing.pem",
                keys.landingSigningPem.toString());
        settings.put("sdh.crypto.keys.recipient.kid", TestKeys.INBOX_KID);
        settings.put("sdh.crypto.keys.recipient.pem",
                keys.inboxPublicPem.toString());
        settings.put("sdh.crypto.keys.recipient.sha256",
                KeyMaterial.fingerprint(keys.inbox.getPublic()));
        settings.putAll(stack.landingQueue(pipeline));
        say("Starting the " + pipeline + " landing node ("
                + DemoApp.jar("landing-connector").getFileName() + ")");
        DemoApp app = new DemoApp("landing",
                DemoApp.jar("landing-connector"), extras, settings,
                WORK.resolve("logs").resolve(pipeline + "-landing.log"));
        app.await("Started LandingConnectorApplication",
                Duration.ofMinutes(2));
        return app;
    }

    /**
     * The settings both apps of a pipeline share: storage, lease, the
     * landed-items KEK, telemetry and logging.
     *
     * @param pipeline the pipeline
     * @return the settings, to add to
     */
    Map<String, String> common(String pipeline)
    {
        Map<String, String> settings = new LinkedHashMap<>(stack.storage());
        settings.put("sdh.storage.catalog-name", "sdh");
        settings.put("sdh.maintenance.enabled", "false");
        settings.put("sdh.crypto.keystore.path", keys.keystore.toString());
        settings.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
        settings.put("sdh.crypto.keys.landed-items-kek", TestKeys.LANDED);
        settings.put("sdh.crypto.keys.signing.passphrase",
                TestKeys.SIGNING_PASSPHRASE);
        settings.put("sdh.telemetry.otlp.endpoint", otlp.endpoint());
        settings.put("sdh.telemetry.metric-interval",
                METRIC_INTERVAL.toSeconds() + "s");
        // Export spans within half a second of an item finishing, rather
        // than Boot's default 5 s.
        settings.put("management.opentelemetry.tracing.export"
                + ".schedule-delay", "500ms");
        // The apps' own log lines, with Boot's startup lines; libraries
        // only when they warn.
        settings.put("logging.level.root", "WARN");
        settings.put("logging.level.com.sdh", "INFO");
        settings.put("logging.level.sdh", "INFO");
        settings.put("logging.level.org.springframework.boot", "INFO");
        return settings;
    }

    /**
     * A pipeline's landing namespace.
     *
     * @param pipeline the pipeline
     * @return the namespace
     */
    static String landingNamespace(String pipeline)
    {
        return pipeline.replace('-', '_') + "_landing";
    }

    /**
     * The class path extras the apps need for tables on local disk: the
     * compiled {@link DemoLocalFileIO}, alone in a directory of its own,
     * so that nothing else of the tests reaches the apps.
     *
     * @return the directory
     * @throws IOException if it can't be made
     */
    static Path extras() throws IOException
    {
        String name = DemoLocalFileIO.class.getName().replace('.', '/')
                + ".class";
        Path extras = WORK.resolve("app-extras");
        Path target = extras.resolve(name);
        Files.createDirectories(target.getParent());
        try (InputStream input = DemoLocalFileIO.class.getClassLoader()
                .getResourceAsStream(name))
        {
            Files.copy(input, target);
        }
        return extras;
    }

    // ---- downstream ----

    /**
     * Downstream: decode every message of a pipeline (decrypt, check the
     * signature) and read each record back out of the Iceberg tables
     * with connector-queue-reader, as a consumer would. Converted
     * documents are written to {@code target/demo/output/<pipeline>/} and
     * an excerpt of each is shown; unconverted ones are kept in their
     * original form.
     *
     * @param pipeline the pipeline
     * @throws Exception if a message can't be decoded or a record read
     */
    void readBack(String pipeline) throws Exception
    {
        banner(pipeline + ": downstream reads the converted documents back");
        Map<UUID, Outcome> byId = new LinkedHashMap<>();
        int expected = 0;
        for (Outcome outcome : outcomes)
        {
            if (outcome.pipeline().equals(pipeline)
                    && outcome.itemId() != null)
            {
                byId.put(outcome.itemId(), outcome);
                expected += outcome.messages();
            }
        }
        List<String> bodies = stack.receive(pipeline, expected,
                Duration.ofSeconds(60));
        say(bodies.size() + " messages received (" + expected
                + " distinct expected), each a signed JWS inside a JWE:"
                + " decrypted with the downstream key and checked against"
                + " the splitter's signing key");
        CommonSettings.Crypto crypto = new CommonSettings.Crypto("local",
                "local", new CommonSettings.Keystore(keys.keystore,
                        TestKeys.KS_PASSWORD),
                keys.splitterKeys(
                        KeyMaterial.fingerprint(keys.recipient.getPublic()),
                        keys.signingPem, null),
                new CommonSettings.Aws(null, null),
                new CommonSettings.Gcp(null),
                new CommonSettings.Vault(null, null, null, "transit", null));
        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", keys.recipient.getPrivate())
                .trustedSigner("connector-2026",
                        (ECPublicKey) keys.signing.getPublic())
                .catalog(stack.newCatalogClient())
                .expectedCatalogName("sdh")
                .keyProvider(KeyMaterial.load(crypto).wrap())
                .build())
        {
            Map<String, Integer> byType = new LinkedHashMap<>();
            List<RecordInStorage> records = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            int repeats = 0;
            for (String body : bodies)
            {
                ConnectorMessage message = reader.decode(body.getBytes(UTF_8));
                // Delivery is at least once: a consumer drops a message ID
                // it has already handled.
                if (!seen.add(message.messageId()))
                {
                    repeats++;
                    continue;
                }
                byType.merge(message.type().wireName(), 1, Integer::sum);
                if (message instanceof RecordInStorage record
                        && byId.containsKey(record.itemId()))
                {
                    records.add(record);
                }
            }
            say("By type: " + byType);
            if (repeats > 0)
            {
                say(repeats + " repeated message ID(s) dropped. Delivery is"
                        + " at least once: a file that fails announces its"
                        + " committed records again (publish.republish) in"
                        + " case the first send was lost, and a record with"
                        + " the same content as an earlier one has the same"
                        + " ID. Consumers ignore an ID they've already"
                        + " handled.");
            }
            int distinct = seen.size();
            int shared = byId.values().stream()
                    .mapToInt(outcome -> outcome.file().repeats()).sum();
            say(distinct != expected
                    ? "MISMATCH: " + distinct + " distinct messages, "
                            + expected + " expected"
                    : shared == 0 ? distinct + " distinct messages, as"
                            + " expected"
                    : distinct + " distinct messages, as expected: " + shared
                            + " record(s) had the same content as an"
                            + " earlier item's, so the same ID, and came"
                            + " once. Artemis drops such a repeat by its"
                            + " message ID; Kafka delivers it, and the"
                            + " consumer drops it.");
            List<UUID> order = new ArrayList<>(byId.keySet());
            records.sort(Comparator.comparing((RecordInStorage record) ->
                    order.indexOf(record.itemId()))
                    .thenComparingLong(RecordInStorage::seq));
            for (RecordInStorage record : records)
            {
                show(reader, record, pipeline,
                        byId.get(record.itemId()).file());
            }
        }
    }

    /**
     * Reads one record back, writes it to the output directory and shows
     * an excerpt.
     *
     * @param reader   the queue reader
     * @param record   the record's message
     * @param pipeline the pipeline
     * @param file     the item it came from
     * @throws IOException if the record can't be read or written
     */
    void show(QueueReader reader, RecordInStorage record, String pipeline,
            DemoFile file) throws IOException
    {
        byte[] content = reader.readRecord(record, 16 << 20);
        String stem = file.name().replaceFirst("\\.xml$", "");
        Path dir = Files.createDirectories(OUTPUT.resolve(pipeline)
                .resolve(stem));
        say("");
        if (record instanceof RecordStored stored)
        {
            String pretty = JSON.readTree(content).toPrettyString();
            Path out = Files.writeString(
                    dir.resolve("record-" + record.seq() + ".json"), pretty);
            say(String.format(Locale.ROOT, "--- %s, record %d: converted"
                    + " (%s), %,d bytes of JSON from %,d bytes of"
                    + " C-CDA at offset %,d -> %s", file.name(),
                    record.seq(), stored.mapping(), content.length,
                    record.sourceLength(), record.sourceOffset(),
                    WORK.relativize(out)));
            excerpt(pretty, 12);
        }
        else if (record instanceof RecordFailed failed)
        {
            Path out = Files.write(dir.resolve(
                    "record-" + record.seq() + ".original.xml"), content);
            say(String.format(Locale.ROOT, "--- %s, record %d: NOT"
                    + " converted (%s); stored in its original form,"
                    + " %,d bytes at offset %,d -> %s", file.name(),
                    record.seq(), failed.errorCode(), content.length,
                    record.sourceOffset(), WORK.relativize(out)));
            excerpt(new String(content, UTF_8), 8);
        }
    }

    // ---- the summary ----

    /** Prints a table of what became of each item. */
    void summary()
    {
        banner("Summary");
        String format = "  %-10s %-34s %-36s %-30s %-9s %8s %5s %s";
        say(String.format(Locale.ROOT, format, "Pipeline", "Item", "Item ID",
                "Landing", "Splitter", "Records", "Raw", "As expected"));
        for (Outcome outcome : outcomes)
        {
            boolean completed = outcome.status().equals("COMPLETED");
            say(String.format(Locale.ROOT, format, outcome.pipeline(),
                    outcome.file().name(), outcome.itemId() == null ? "-"
                            : outcome.itemId().toString(),
                    outcome.landing(), outcome.itemId() == null ? "-"
                            : outcome.status(),
                    completed ? Long.toString(outcome.records()) : "-",
                    completed ? Long.toString(outcome.failed()) : "-",
                    outcome.asExpected() ? "yes" : "NO"));
        }
        long unexpected = outcomes.stream()
                .filter(outcome -> !outcome.asExpected()).count();
        say(unexpected == 0 ? "Every item ended as expected."
                : unexpected + " item(s) did NOT end as expected.");
    }

    // ---- console ----

    /**
     * Keeps the demo's own libraries (Testcontainers, the broker, Iceberg)
     * to warnings, since no Spring context sets their levels here.
     */
    static void quietLogging()
    {
        ((Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME))
                .setLevel(Level.WARN);
    }

    /**
     * Prints the first lines of a text, each cut at 140 characters.
     *
     * @param text  the text
     * @param lines how many lines to show
     */
    static void excerpt(String text, int lines)
    {
        String[] all = text.split("\n", -1);
        for (int index = 0; index < Math.min(lines, all.length); index++)
        {
            say("    " + (all[index].length() > 140
                    ? all[index].substring(0, 140) + " ..." : all[index]));
        }
        if (all.length > lines)
        {
            say(String.format(Locale.ROOT, "    ... (%,d more lines)",
                    all.length - lines));
        }
    }

    /**
     * Prints a title between two rules.
     *
     * @param title the title
     */
    static void banner(String title)
    {
        String rule = "=".repeat(Math.max(20, Math.min(110,
                title.length() + 8)));
        System.out.println("\n" + rule + "\n=== " + title + "\n" + rule);
        System.out.flush();
    }

    /**
     * Prints a line of the demo's own commentary.
     *
     * @param line the line
     */
    static void say(String line)
    {
        System.out.println("[demo] " + line);
        System.out.flush();
    }

    /**
     * A duration setting, such as {@code 3s}.
     *
     * @param property the system property
     * @param fallback its value when unset
     * @return the duration
     */
    static Duration duration(String property, String fallback)
    {
        return Duration.parse("PT" + System.getProperty(property, fallback)
                .toUpperCase(Locale.ROOT));
    }

    /**
     * How many files are in a directory.
     *
     * @param directory the directory
     * @return the count
     * @throws IOException if it can't be listed
     */
    static long count(Path directory) throws IOException
    {
        try (Stream<Path> files = Files.list(directory))
        {
            return files.count();
        }
    }

    /**
     * Deletes a directory and everything in it, if it exists.
     *
     * @param root the directory
     * @throws IOException if something can't be deleted
     */
    static void deleteTree(Path root) throws IOException
    {
        if (!Files.exists(root))
        {
            return;
        }
        try (Stream<Path> paths = Files.walk(root))
        {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList())
            {
                Files.delete(path);
            }
        }
    }
}
