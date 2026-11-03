/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.file.demo;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionFailed;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.core.checkpoint.Checkpoint;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.file.TestKeys;
import com.sdh.healthcare.connector.file.TestLanding;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.file.demo.DemoFiles.DemoFile;
import com.sdh.healthcare.connector.file.runtime.ConnectorRuntime;
import com.sdh.healthcare.connector.runtime.telemetry.AllowListSpanExporter;
import com.sdh.healthcare.connector.runtime.telemetry.AuditOnlyLogRecordExporter;
import com.sdh.healthcare.connector.reader.QueueReader;
import io.opentelemetry.sdk.logs.export.LogRecordExporter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.stream.Stream;
import org.apache.camel.CamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * A demonstration of the splitter at work (design §15.3): C-CDA files are landed one by one, as a landing node would
 * land them, and each is announced to the splitter with an {@code item.landed} message. The console shows the
 * splitter's log, its audit events, and its OpenTelemetry traces and metrics as each file goes through. At the
 * end, a downstream consumer reads the converted documents back from the sealed messages and the Iceberg tables, and
 * excerpts are shown. Some files fail, in the three ways a C-CDA file can: an encoding the connector refuses, a file
 * that breaks off part-way, and a document that splits but can't be converted.
 *
 * <p>Run with {@code -Pdemo} ({@code ./mvnw -Pdemo -pl file-connector -am verify}), or from NetBeans (Run Maven >
 * Demo). Never part of a normal build or the integration tests. Settings: {@code demo.stack} {@code local} (default:
 * one JVM, no Docker) or {@code vm} (Postgres, SeaweedFS and Kafka in the minikube VM); {@code demo.pause} between
 * files (default 3s); {@code demo.metric-interval} (default 15s). Output in {@code file-connector/target/demo/}.
 * The landing node itself isn't run: {@link TestLanding} stands in for it, writing to the same tables.
 *
 * <p>The samples are synthetic or de-identified; none contains PHI. The connector's own output stays PHI-free either
 * way: the console telemetry goes through the same filters as the OTLP export.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
// Registered before Spring's extension, so it runs after Spring has closed the connector: the stack it shuts down
// (Postgres, the broker, the VM) must outlive the connector's own shutdown, which releases the lease.
@ExtendWith(PipelineDemo.StackCloser.class)
@SpringBootTest(properties = {
    "camel.main.run-controller=false",
    // Export spans within half a second of a file finishing, rather than Boot's default 5 s.
    "management.opentelemetry.tracing.export.schedule-delay=500ms",
    // The connector's own log lines; libraries only when they warn.
    "logging.level.root=WARN",
    "logging.level.com.sdh=INFO",
    "logging.level.sdh=INFO"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Import(PipelineDemo.ConsoleExporters.class)
class PipelineDemo {
    static final Path WORK = Path.of("target", "demo");
    /** Where the landing stand-in sends item.landed, and the splitter consumes from. */
    static final String LANDED_QUEUE = "seda:sdh-demo-landed";
    static final Path OUTPUT = WORK.resolve("output").toAbsolutePath();
    static final Duration PAUSE = Duration.parse("PT" + System.getProperty("demo.pause", "3s").toUpperCase(Locale.ROOT));
    static final ObjectMapper JSON = new ObjectMapper();
    static final String INTAKE = "demo";
    static final TestKeys KEYS;
    static final DemoStack STACK;
    static final List<DemoFile> FILES;

    static {
        try {
            deleteTree(WORK);
            Files.createDirectories(OUTPUT);
            KEYS = new TestKeys(Files.createDirectories(WORK.resolve("keys")));
            FILES = DemoFiles.build();
            banner("Healthcare file connector demo");
            STACK = DemoStack.create(System.getProperty("demo.stack", "local"), WORK);
            say("Running against: " + STACK.describe());
            say("Files to land, one at a time:");
            for (DemoFile f : FILES) {
                say(String.format(Locale.ROOT, "  %-32s %,9d bytes  %s", f.name(), f.content().length, f.expected()));
            }
            say("Each file shows the connector's log lines and [otel log] audit events as it runs, then its [otel trace]."
                    + " [otel metrics] are printed every " + System.getProperty("demo.metric-interval", "15s") + ".");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Console exporters, wrapped in the production filters. They take the place of the OTLP exporters, which only
     * exist when {@code sdh.telemetry.otlp.endpoint} is set.
     */
    @TestConfiguration
    static class ConsoleExporters {
        @Bean
        SpanExporter demoSpanExporter() {
            return new AllowListSpanExporter(new ConsoleTelemetry.SpanTrees());
        }

        @Bean
        LogRecordExporter demoLogRecordExporter() {
            return new AuditOnlyLogRecordExporter(new ConsoleTelemetry.LogLines());
        }

        @Bean(destroyMethod = "close")
        SdkMeterProvider demoMeterProvider(Resource resource) {
            Duration interval = Duration.parse("PT" + System.getProperty("demo.metric-interval", "15s")
                    .toUpperCase(Locale.ROOT));
            return SdkMeterProvider.builder().setResource(resource)
                    .registerMetricReader(PeriodicMetricReader.builder(new ConsoleTelemetry.MetricTable())
                            .setInterval(interval).build())
                    .build();
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) throws Exception {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("sdh.landing.intake", INTAKE);
        p.put("sdh.landing.queue-uri", LANDED_QUEUE);
        p.put("sdh.landing.idle-wait", "200ms");
        p.put("sdh.split.format", "ccda");
        p.put("sdh.storage.catalog-name", "sdh");
        // A commit per document, so each one is stored (and announced) as soon as it's split: a file that fails
        // part-way keeps what came before the failure.
        p.put("sdh.storage.commit.max-sections", "1");
        p.put("sdh.maintenance.enabled", "false");
        p.put("sdh.crypto.keystore.path", KEYS.keystore.toString());
        p.put("sdh.crypto.keystore.password", TestKeys.KS_PASSWORD);
        p.put("sdh.crypto.keys.sections-kek", TestKeys.SECTIONS);
        p.put("sdh.crypto.keys.landed-items-kek", TestKeys.LANDED);
        p.put("sdh.crypto.keys.signing.kid", "connector-2026");
        p.put("sdh.crypto.keys.signing.pem", KEYS.signingPem.toString());
        p.put("sdh.crypto.keys.signing.passphrase", TestKeys.SIGNING_PASSPHRASE);
        p.put("sdh.crypto.keys.recipient.kid", "downstream-2026");
        p.put("sdh.crypto.keys.recipient.pem", KEYS.recipientPem.toString());
        p.put("sdh.crypto.keys.recipient.sha256", KeyMaterial.fingerprint(KEYS.recipient.getPublic()));
        p.put("sdh.crypto.keys.inbox.kid", TestKeys.INBOX_KID);
        p.put("sdh.crypto.keys.inbox.pem", KEYS.inboxPem.toString());
        p.put("sdh.crypto.keys.inbox.passphrase", TestKeys.INBOX_PASSPHRASE);
        p.put("sdh.crypto.keys.landing-signer.kid", TestKeys.LANDING_SIGNER_KID);
        p.put("sdh.crypto.keys.landing-signer.pem", KEYS.landingSignerPem.toString());
        p.putAll(STACK.properties());
        p.forEach((k, v) -> r.add(k, () -> v));
    }

    @Autowired
    ConnectorRuntime runtime;

    @Autowired
    CamelContext camel;

    @Autowired
    SdkMeterProvider meters;

    /** One landed file and what became of it: the splitter's checkpoint once it finished. */
    record Outcome(DemoFile file, UUID fileId, Checkpoint checkpoint, long failedSections, long millis) {
        String status() {
            return checkpoint.state().name();
        }

        boolean asExpected() {
            return status().equals(file.status())
                    && (!file.status().equals("COMPLETED")
                            || checkpoint.sectionCount() == file.sections()
                                    && failedSections == file.failedSections());
        }
    }

    /** Shuts the stack down once Spring has closed the connector. */
    static final class StackCloser implements AfterAllCallback {
        @Override
        public void afterAll(ExtensionContext context) throws Exception {
            STACK.close();
            banner("Demo finished");
        }
    }

    @Test
    void demo() throws Exception {
        TestLanding landing = new TestLanding(STACK.catalog(), runtime.keys().wrap(), KEYS, INTAKE);
        List<Outcome> outcomes = new ArrayList<>();
        int n = 0;
        for (DemoFile f : FILES) {
            outcomes.add(deliver(landing, f, ++n));
            Thread.sleep(PAUSE.toMillis());
        }
        meters.forceFlush().join(10, java.util.concurrent.TimeUnit.SECONDS);
        summary(outcomes);
        readBack(outcomes);
    }

    Outcome deliver(TestLanding landing, DemoFile f, int n) throws Exception {
        banner(String.format(Locale.ROOT, "File %d of %d: %s", n, FILES.size(), f.name()));
        say("Expected: " + f.expected());
        long start = System.currentTimeMillis();
        LandedItem item = landing.land(f.content(), f.name());
        landing.announce(camel, LANDED_QUEUE, item);
        UUID fileId = item.itemId();
        say("Landed " + f.name() + " (" + f.content().length + " bytes) as item " + fileId
                + ", and announced it to the splitter");
        Checkpoint done = null;
        while (done == null && System.currentTimeMillis() - start < 180_000) {
            Thread.sleep(200);
            done = runtime.processor().last()
                    .filter(c -> c.fileId().equals(fileId) && !c.state().inProgress()).orElse(null);
        }
        if (done == null) {
            throw new IllegalStateException(f.name() + " wasn't finished within 3 minutes");
        }
        long millis = System.currentTimeMillis() - start;
        Outcome o = new Outcome(f, fileId, done, runtime.processor().failedSectionCount(fileId), millis);
        String detail = done.failure() == null
                ? done.sectionCount() + " sections, " + o.failedSections() + " stored unconverted"
                : done.failure().code() + " at byte " + done.failure().offset() + " in stage "
                        + done.failure().stage() + "; the original stays in landing";
        say(String.format(Locale.ROOT, "Result: %s is item %s: %s (%s) after %.1f s%s", f.name(), fileId,
                o.status(), detail, millis / 1000.0, o.asExpected() ? "" : "  (NOT AS EXPECTED)"));
        return o;
    }

    void summary(List<Outcome> outcomes) {
        banner("Summary");
        say(String.format(Locale.ROOT, "  %-32s %-36s %-9s %8s %8s %s", "File", "File ID", "Status", "Sections",
                "Raw", "As expected"));
        for (Outcome o : outcomes) {
            boolean done = o.status().equals("COMPLETED");
            say(String.format(Locale.ROOT, "  %-32s %-36s %-9s %8s %8s %s", o.file().name(), o.fileId(), o.status(),
                    done ? String.valueOf(o.checkpoint().sectionCount()) : "-",
                    done ? String.valueOf(o.failedSections()) : String.valueOf(o.checkpoint().failure().code()),
                    o.asExpected() ? "yes" : "NO"));
        }
    }

    /**
     * Downstream: decode every message (decrypt, check the signature) and read each section back out of the Iceberg
     * tables with connector-queue-reader, as a consumer would. Converted documents are written to target/demo/output/
     * and an excerpt of each is shown; unconverted ones are kept in their original form.
     */
    void readBack(List<Outcome> outcomes) throws Exception {
        banner("Downstream: reading the converted documents back");
        Map<UUID, Outcome> byId = new LinkedHashMap<>();
        int expected = 0;
        for (Outcome o : outcomes) {
            byId.put(o.fileId(), o);
            // file.started, then each committed section; file.completed only for completed files.
            expected += o.status().equals("COMPLETED") ? 2 + o.checkpoint().sectionCount()
                    : 1 + Math.max(0, o.checkpoint().committedSeq() + 1);
        }
        List<String> bodies = STACK.receive(camel, expected, Duration.ofSeconds(60));
        say(bodies.size() + " messages received (" + expected + " distinct expected), each a signed JWS inside a"
                + " JWE: decrypted with the downstream key and checked against the connector's signing key");
        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", KEYS.recipient.getPrivate())
                .trustedSigner("connector-2026", (ECPublicKey) KEYS.signing.getPublic())
                .catalog(STACK.newCatalogClient())
                .expectedCatalogName("sdh")
                .keyProvider(runtime.keys().wrap())
                .build()) {
            Map<String, Integer> byType = new LinkedHashMap<>();
            List<SectionInStorage> sections = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            int repeats = 0;
            for (String body : bodies) {
                ConnectorMessage m = reader.decode(body.getBytes(UTF_8));
                // Delivery is at least once: a consumer drops a message ID it has already handled.
                if (!seen.add(m.messageId())) {
                    repeats++;
                    continue;
                }
                byType.merge(m.type().wireName(), 1, Integer::sum);
                if (m instanceof SectionInStorage s) {
                    sections.add(s);
                }
            }
            say("By type: " + byType);
            if (repeats > 0) {
                say(repeats + " repeated message ID(s) dropped. Delivery is at least once: a file that fails"
                        + " announces its committed sections again (publish.republish) in case the first send was lost,"
                        + " and consumers ignore an ID they've already handled.");
            }
            sections.sort(Comparator.comparing((SectionInStorage s) -> FILES.indexOf(byId.get(s.fileId()).file()))
                    .thenComparingLong(SectionInStorage::seq));
            for (SectionInStorage s : sections) {
                show(reader, s, byId.get(s.fileId()).file());
            }
        }
        say("");
        say("Converted documents: " + OUTPUT);
    }

    void show(QueueReader reader, SectionInStorage s, DemoFile file) throws IOException {
        byte[] content = reader.readSection(s, 16 << 20);
        String stem = file.name().replaceFirst("\\.xml$", "");
        Path dir = Files.createDirectories(OUTPUT.resolve(stem));
        say("");
        if (s instanceof SectionStored ok) {
            String pretty = JSON.readTree(content).toPrettyString();
            Path out = Files.writeString(dir.resolve("section-" + s.seq() + ".json"), pretty);
            say(String.format(Locale.ROOT, "--- %s, section %d: converted (%s), %,d bytes of JSON from %,d bytes of"
                    + " C-CDA at offset %,d -> %s", file.name(), s.seq(), ok.mapping(), content.length,
                    s.sourceLength(), s.sourceOffset(), OUTPUT.getParent().relativize(out)));
            excerpt(pretty, 16);
        } else if (s instanceof SectionFailed bad) {
            Path out = Files.write(dir.resolve("section-" + s.seq() + ".original.xml"), content);
            say(String.format(Locale.ROOT, "--- %s, section %d: NOT converted (%s); stored in its original form,"
                    + " %,d bytes at offset %,d -> %s", file.name(), s.seq(), bad.errorCode(), content.length,
                    s.sourceOffset(), OUTPUT.getParent().relativize(out)));
            excerpt(new String(content, UTF_8), 8);
        }
    }

    static void excerpt(String text, int lines) {
        String[] all = text.split("\n", -1);
        for (int i = 0; i < Math.min(lines, all.length); i++) {
            say("    " + (all[i].length() > 140 ? all[i].substring(0, 140) + " ..." : all[i]));
        }
        if (all.length > lines) {
            say(String.format(Locale.ROOT, "    ... (%,d more lines)", all.length - lines));
        }
    }

    static void banner(String title) {
        String rule = "=".repeat(Math.max(20, Math.min(110, title.length() + 8)));
        System.out.println("\n" + rule + "\n=== " + title + "\n" + rule);
        System.out.flush();
    }

    static void say(String line) {
        System.out.println("[demo] " + line);
        System.out.flush();
    }

    static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(p);
            }
        }
    }
}
