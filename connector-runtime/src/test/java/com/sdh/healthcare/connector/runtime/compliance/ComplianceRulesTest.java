/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.runtime.compliance.ComplianceRules.Finding;
import com.sdh.healthcare.connector.runtime.compliance.ComplianceRules.Outcome;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * The compliance profile's rules (design §7.6), on settings as a node's
 * environment gives them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ComplianceRulesTest
{

    /** A JVM started with the image's options (design §14.1). */
    private static final Function<String, Optional<String>> SAFE_JVM =
            option -> Optional.of(switch (option)
            {
                case "ErrorFile" -> "/dev/null";
                case "DisableAttachMechanism" -> "true";
                default -> "false";
            });

    /** Creates the tests; JUnit runs each test on a new instance. */
    ComplianceRulesTest()
    {
    }

    /**
     * A node's settings that pass: TLS everywhere, keys in a KMS, an audit
     * export.
     *
     * @return the settings, to change one at a time
     */
    private static Map<String, String> passing()
    {
        Map<String, String> settings = new TreeMap<>();
        settings.put("sdh.lease.jdbc.url",
                "jdbc:postgresql://db:5432/sdh?sslmode=verify-full");
        settings.put("sdh.storage.catalog.uri",
                "jdbc:postgresql://db:5432/sdh");
        settings.put("sdh.storage.catalog.jdbc.sslmode", "verify-ca");
        settings.put("sdh.storage.catalog.s3.endpoint", "https://s3.local");
        settings.put("sdh.stage.input.uri",
                "kafka:p.split?brokers=k:9093&securityProtocol=SASL_SSL");
        settings.put("sdh.stage.output.uri",
                "kafka:p.records?brokers=k:9093&securityProtocol=SSL");
        settings.put("sdh.crypto.wrap-provider", "aws-kms");
        settings.put("sdh.crypto.sign-provider", "aws-kms");
        settings.put("sdh.crypto.vault.address", "https://vault:8200");
        settings.put("sdh.telemetry.otlp.endpoint", "http://collector:4318");
        return settings;
    }

    /**
     * What the profile refuses, as check and setting.
     *
     * @param settings the settings
     * @return the refusals
     */
    private static List<Finding> refused(Map<String, String> settings)
    {
        return ComplianceRules.evaluate(settings, SAFE_JVM).refused();
    }

    /** Settings with TLS everywhere and keys in a KMS pass by default. */
    @Test
    void aHardenedNodePassesUnderTheDefault()
    {
        Outcome outcome = ComplianceRules.evaluate(passing(), SAFE_JVM);

        assertThat(outcome.profile()).isEqualTo(ComplianceProfile.HIPAA);
        assertThat(outcome.passed()).isTrue();
        assertThat(outcome.waived()).isEmpty();
    }

    /**
     * The CLI may run an operation needing four eyes only with the profile
     * off or the check waived; the waiver is found, and shown as waived,
     * but never refuses a start.
     */
    @Test
    void theCliRunsFourEyesOperationsOnlyIfWaived()
    {
        String waiver = ComplianceRules.WAIVE + "cli-four-eyes";
        assertThat(ComplianceRules.cliFourEyesAllowed(name -> null))
                .isFalse();
        assertThat(ComplianceRules.cliFourEyesAllowed(
                Map.of(ComplianceRules.PROFILE, "none")::get)).isTrue();
        assertThat(ComplianceRules.cliFourEyesAllowed(
                Map.of(waiver, "Recovery drill")::get)).isTrue();
        assertThatThrownBy(() -> ComplianceRules.cliFourEyesAllowed(
                Map.of(waiver, " ")::get))
                .isInstanceOf(IllegalArgumentException.class);

        Map<String, String> settings = passing();
        assertThat(ComplianceRules.evaluate(settings, SAFE_JVM).waived())
                .isEmpty();
        settings.put(waiver, "Recovery drill");
        Outcome outcome = ComplianceRules.evaluate(settings, SAFE_JVM);
        assertThat(outcome.passed()).isTrue();
        assertThat(outcome.waived()).containsEntry(new Finding(
                ComplianceCheck.CLI_FOUR_EYES, waiver), "Recovery drill");
    }

    /** A plaintext endpoint anywhere is refused, by setting name only. */
    @Test
    void plaintextEndpointsAreRefused()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.storage.catalog.s3.endpoint",
                "http://seaweedfs:8333");
        settings.put("sdh.crypto.vault.address", "HTTP://vault:8200");
        settings.put("sdh.queue.artemis.broker-url", "tcp://artemis:61616");
        settings.put("sdh.storage.catalog.adls.connection-string",
                "DefaultEndpointsProtocol=http;AccountName=a;AccountKey=k;");

        assertThat(refused(settings)).containsExactlyInAnyOrder(
                new Finding(ComplianceCheck.TRANSPORT_PLAINTEXT,
                        "sdh.storage.catalog.s3.endpoint"),
                new Finding(ComplianceCheck.TRANSPORT_PLAINTEXT,
                        "sdh.crypto.vault.address"),
                new Finding(ComplianceCheck.TRANSPORT_PLAINTEXT,
                        "sdh.queue.artemis.broker-url"),
                new Finding(ComplianceCheck.TRANSPORT_PLAINTEXT,
                        "sdh.storage.catalog.adls.connection-string"));
    }

    /** Artemis with TLS, and HTTPS, pass. */
    @Test
    void encryptedEndpointsPass()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.queue.artemis.broker-url",
                "tcp://artemis:61617?sslEnabled=true&trustStorePath=/t.p12");
        settings.put("sdh.landing.http.oauth2.issuer-uri",
                "https://idp.example/realms/sdh");

        assertThat(refused(settings)).isEmpty();
    }

    /** The audit export may be plain HTTP, but must be set. */
    @Test
    void theAuditExportMustBeSet()
    {
        Map<String, String> settings = passing();
        settings.remove("sdh.telemetry.otlp.endpoint");

        assertThat(refused(settings)).containsExactly(
                new Finding(ComplianceCheck.AUDIT_EXPORT,
                        "sdh.telemetry.otlp.endpoint"));
    }

    /**
     * A control API (CONTROL-API-DESIGN.md §12) without TLS, or accepting
     * tokens issued for longer than an hour, is refused; a worker's
     * settings aren't judged as a control API's, and an hour passes.
     */
    @Test
    void aControlApiNeedsTlsAndShortTokens()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.control.tls.enabled", "false");
        settings.put("sdh.control.auth.jwt.max-lifetime", "2h");
        assertThat(refused(settings)).isEmpty();

        settings.put("sdh.control.mode", "true");
        assertThat(refused(settings)).containsExactlyInAnyOrder(
                new Finding(ComplianceCheck.CONTROL_TLS,
                        "sdh.control.tls.enabled"),
                new Finding(ComplianceCheck.CONTROL_TOKEN_LIFETIME,
                        "sdh.control.auth.jwt.max-lifetime"));

        settings.put("sdh.control.tls.enabled", "true");
        settings.put("sdh.control.auth.jwt.max-lifetime", "60m");
        assertThat(refused(settings)).isEmpty();
        settings.remove("sdh.control.auth.jwt.max-lifetime");
        assertThat(refused(settings)).isEmpty();
        settings.put("sdh.control.auth.jwt.max-lifetime", "a while");
        assertThat(refused(settings)).containsExactly(new Finding(
                ComplianceCheck.CONTROL_TOKEN_LIFETIME,
                "sdh.control.auth.jwt.max-lifetime"));
    }

    /** The durable audit journal is on unless turned off, which is refused. */
    @Test
    void theAuditJournalMustBeOn()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.telemetry.journal.enabled", "false");

        assertThat(refused(passing())).isEmpty();
        assertThat(refused(settings)).containsExactly(
                new Finding(ComplianceCheck.AUDIT_JOURNAL,
                        "sdh.telemetry.journal.enabled"));
    }

    /** Postgres must verify the server, in the URL or the catalog's map. */
    @Test
    void postgresMustVerifyTheServer()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.lease.jdbc.url",
                "jdbc:postgresql://db:5432/sdh?sslmode=require");
        settings.remove("sdh.storage.catalog.jdbc.sslmode");

        assertThat(refused(settings)).containsExactlyInAnyOrder(
                new Finding(ComplianceCheck.TRANSPORT_DATABASE,
                        "sdh.lease.jdbc.url"),
                new Finding(ComplianceCheck.TRANSPORT_DATABASE,
                        "sdh.storage.catalog.uri"));
    }

    /** A database the profile doesn't know is refused as unchecked. */
    @Test
    void anUnknownDatabaseIsUnchecked()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.storage.catalog.uri", "jdbc:oracle:thin:@db:1521");

        assertThat(refused(settings)).containsExactly(
                new Finding(ComplianceCheck.TRANSPORT_UNCHECKED,
                        "sdh.storage.catalog.uri"));
    }

    /** Kafka must use TLS, set on the URI or on the component. */
    @Test
    void kafkaMustUseTls()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.stage.input.uri", "kafka:p.split?brokers=k:9092");
        settings.put("sdh.stage.dlq.uri",
                "kafka:p.dlq?brokers=k:9092&securityProtocol=PLAINTEXT");

        assertThat(refused(settings)).containsExactlyInAnyOrder(
                new Finding(ComplianceCheck.TRANSPORT_BROKER,
                        "sdh.stage.input.uri"),
                new Finding(ComplianceCheck.TRANSPORT_BROKER,
                        "sdh.stage.dlq.uri"));

        settings.remove("sdh.stage.dlq.uri");
        settings.put("camel.component.kafka.security-protocol", "SASL_SSL");
        assertThat(refused(settings)).isEmpty();
    }

    /** RabbitMQ, SQS and Pub/Sub each have their own way of going plain. */
    @Test
    void otherBrokersAreChecked()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.queue.uri", "spring-rabbitmq:sdh?routingKey=a");
        settings.put("sdh.results.queue.uri",
                "aws2-sqs:r.fifo?overrideEndpoint=true"
                + "&uriEndpointOverride=http://localstack:4566");
        settings.put("sdh.stage.output.uri",
                "google-pubsub:proj:topic?authenticate=false");

        assertThat(refused(settings)).containsExactlyInAnyOrder(
                new Finding(ComplianceCheck.TRANSPORT_BROKER,
                        "sdh.queue.uri"),
                new Finding(ComplianceCheck.TRANSPORT_BROKER,
                        "sdh.results.queue.uri"),
                new Finding(ComplianceCheck.TRANSPORT_BROKER,
                        "sdh.stage.output.uri"));

        settings.put("sdh.landing.broker.uri", "spring-rabbitmq:d?queues=q");
        assertThat(refused(settings)).contains(new Finding(
                ComplianceCheck.TRANSPORT_BROKER, "sdh.landing.broker.uri"));
        settings.put("sdh.landing.broker.rabbitmq.ssl", "true");
        assertThat(refused(settings)).doesNotContain(new Finding(
                ComplianceCheck.TRANSPORT_BROKER, "sdh.landing.broker.uri"));

        settings.put("spring.rabbitmq.ssl.enabled", "true");
        settings.put("sdh.results.queue.uri", "aws2-sqs:r.fifo");
        settings.put("sdh.stage.output.uri", "google-pubsub:proj:topic");
        assertThat(refused(settings)).isEmpty();
    }

    /** An emulator's connection string is plaintext, wherever it is set. */
    @Test
    void anAzureEmulatorIsPlaintext()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.stage.input.uri", "azure-servicebus:q?"
                + "connectionString=Endpoint=sb://localhost;"
                + "UseDevelopmentEmulator=true");

        assertThat(refused(settings)).containsExactly(
                new Finding(ComplianceCheck.TRANSPORT_PLAINTEXT,
                        "sdh.stage.input.uri"));
    }

    /** In-process endpoints pass; components it doesn't know don't. */
    @Test
    void unknownComponentsAreUnchecked()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.stage.output.uri", "seda:records");
        settings.put("sdh.stage.dlq.uri", "netty:tcp://host:9000");

        assertThat(refused(settings)).containsExactly(
                new Finding(ComplianceCheck.TRANSPORT_UNCHECKED,
                        "sdh.stage.dlq.uri"));
    }

    /** Local keys are refused, including by default, when unset. */
    @Test
    void localKeysAreRefused()
    {
        Map<String, String> settings = passing();
        settings.remove("sdh.crypto.wrap-provider");
        settings.put("sdh.crypto.sign-provider", "LOCAL");

        assertThat(refused(settings)).containsExactly(
                new Finding(ComplianceCheck.KEYS_LOCAL,
                        "sdh.crypto.wrap-provider"),
                new Finding(ComplianceCheck.KEYS_LOCAL,
                        "sdh.crypto.sign-provider"));
    }

    /** Storage without authentication is refused. */
    @Test
    void storageWithoutAuthenticationIsRefused()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.storage.catalog.gcs.no-auth", "true");

        assertThat(refused(settings)).containsExactly(
                new Finding(ComplianceCheck.STORAGE_NO_AUTH,
                        "sdh.storage.catalog.gcs.no-auth"));
    }

    /** A JVM that may dump memory is refused, option by option. */
    @Test
    void aJvmThatMayDumpMemoryIsRefused()
    {
        Outcome outcome = ComplianceRules.evaluate(passing(),
                option -> option.equals("HeapDumpOnOutOfMemoryError")
                        ? Optional.of("true") : SAFE_JVM.apply(option));
        Outcome unknown = ComplianceRules.evaluate(passing(),
                option -> Optional.empty());

        assertThat(outcome.refused()).containsExactly(
                new Finding(ComplianceCheck.JVM_DUMPS,
                        "-XX:HeapDumpOnOutOfMemoryError"));
        assertThat(unknown.refused()).hasSize(4)
                .allMatch(f -> f.check() == ComplianceCheck.JVM_DUMPS);
    }

    /** A waiver with a reason accepts every finding of its check. */
    @Test
    void aWaiverAcceptsItsCheckWithTheReason()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.crypto.wrap-provider", "local");
        settings.put("sdh.crypto.sign-provider", "local");
        settings.put("sdh.compliance.waive.keys-local",
                "  On-premises HSM not yet procured; risk accepted RA-12  ");

        Outcome outcome = ComplianceRules.evaluate(settings, SAFE_JVM);

        assertThat(outcome.passed()).isTrue();
        assertThat(outcome.waived()).containsOnlyKeys(
                new Finding(ComplianceCheck.KEYS_LOCAL,
                        "sdh.crypto.wrap-provider"),
                new Finding(ComplianceCheck.KEYS_LOCAL,
                        "sdh.crypto.sign-provider"))
                .containsValue("On-premises HSM not yet procured;"
                        + " risk accepted RA-12");
    }

    /** A waiver for another check doesn't accept this one. */
    @Test
    void aWaiverAcceptsOnlyItsOwnCheck()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.crypto.wrap-provider", "local");
        settings.put("sdh.compliance.waive.audit-export", "not needed");

        assertThat(refused(settings)).containsExactly(
                new Finding(ComplianceCheck.KEYS_LOCAL,
                        "sdh.crypto.wrap-provider"));
    }

    /** A waiver must name a check and give a one-line reason. */
    @Test
    void aWaiverMustNameACheckAndAReason()
    {
        Map<String, String> typo = passing();
        typo.put("sdh.compliance.waive.keys-locl", "a reason");
        Map<String, String> blank = passing();
        blank.put("sdh.compliance.waive.keys-local", "  ");
        Map<String, String> twoLines = passing();
        twoLines.put("sdh.compliance.waive.keys-local", "one\ntwo");
        Map<String, String> tooLong = passing();
        tooLong.put("sdh.compliance.waive.keys-local", "x".repeat(201));

        assertThatThrownBy(() -> ComplianceRules.evaluate(typo, SAFE_JVM))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sdh.compliance.waive.keys-locl")
                .hasMessageContaining("keys-local");
        for (Map<String, String> settings : List.of(blank, twoLines, tooLong))
        {
            assertThatThrownBy(() ->
                    ComplianceRules.evaluate(settings, SAFE_JVM))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must give the reason");
        }
    }

    /** Profile none checks nothing; an unknown profile is refused. */
    @Test
    void profileNoneChecksNothing()
    {
        Map<String, String> settings = new TreeMap<>();
        settings.put("sdh.compliance.profile", "None");
        settings.put("sdh.crypto.vault.address", "http://vault:8200");

        Outcome outcome = ComplianceRules.evaluate(settings,
                option -> Optional.empty());

        assertThat(outcome.profile()).isEqualTo(ComplianceProfile.NONE);
        assertThat(outcome.passed()).isTrue();
        assertThatThrownBy(() -> ComplianceRules.evaluate(
                Map.of("sdh.compliance.profile", "strict"), SAFE_JVM))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hipaa or none");
    }

    /** No value ever appears in what the profile reports. */
    @Test
    void messagesNameSettingsNeverValues()
    {
        Map<String, String> settings = passing();
        settings.put("sdh.lease.jdbc.url",
                "jdbc:postgresql://db/sdh?password=s3cret-value");

        Outcome outcome = ComplianceRules.evaluate(settings, SAFE_JVM);
        String message =
                ComplianceConfiguration.ComplianceGuard.refusal(outcome);

        assertThat(message).contains("transport-database: sdh.lease.jdbc.url")
                .doesNotContain("s3cret-value").doesNotContain("db/sdh");
    }
}
