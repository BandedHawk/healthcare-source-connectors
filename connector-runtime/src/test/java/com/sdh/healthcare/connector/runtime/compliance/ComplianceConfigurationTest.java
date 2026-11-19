/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

/**
 * The guard in a Spring context (design §7.6): it reads settings however
 * they are set, and stops the context before any other bean is made.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ComplianceConfigurationTest
{

    /** A context with only the compliance configuration. */
    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withUserConfiguration(ComplianceConfiguration.class);

    /** Creates the tests; JUnit runs each test on a new instance. */
    ComplianceConfigurationTest()
    {
    }

    /** The default profile refuses a plaintext endpoint at startup. */
    @Test
    void theDefaultProfileStopsStartup()
    {
        runner.withPropertyValues("sdh.compliance.profile=",
                "sdh.crypto.vault.address=http://vault:8200")
                .run(context ->
                {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure())
                            .hasMessageContaining(
                                    "sdh.compliance.profile=hipaa")
                            .hasMessageContaining("transport-plaintext:"
                                    + " sdh.crypto.vault.address")
                            .hasMessageContaining("keys-local")
                            .hasMessageNotContaining("http://vault");
                });
    }

    /** Waivers for every check let the context start. */
    @Test
    void waivedChecksLetItStart()
    {
        runner.withPropertyValues("sdh.compliance.profile=hipaa",
                "sdh.crypto.vault.address=http://vault:8200",
                "sdh.compliance.waive.transport-plaintext=test only",
                "sdh.compliance.waive.keys-local=test only",
                "sdh.compliance.waive.audit-export=test only",
                // The test JVM isn't started with the image's options.
                "sdh.compliance.waive.jvm-dumps=test only")
                .run(context ->
                {
                    assertThat(context).hasNotFailed();
                    ComplianceRules.Outcome outcome =
                            ComplianceConfiguration.ComplianceGuard
                                    .lastOutcome();
                    assertThat(outcome.passed()).isTrue();
                    assertThat(outcome.waived()).isNotEmpty();
                });
    }

    /** Settings from environment variables are read by canonical name. */
    @Test
    void settingsFromTheEnvironmentAreChecked()
    {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().replace(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(
                        StandardEnvironment
                                .SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                        Map.of(
                        "SDH_STORAGE_CATALOG_S3_ENDPOINT", "http://s3:8333",
                        "SDH_CRYPTO_WRAP_PROVIDER", "aws-kms",
                        "SDH_CRYPTO_SIGNPROVIDER", "aws-kms",
                        "SDH_COMPLIANCE_WAIVE_AUDIT_EXPORT", "sidecar")));

        Map<String, String> settings =
                ComplianceConfiguration.settings(environment);

        assertThat(settings).containsEntry("sdh.storage.catalog.s3.endpoint",
                "http://s3:8333").containsEntry("sdh.crypto.wrap-provider",
                "aws-kms").containsEntry("sdh.crypto.sign-provider",
                "aws-kms").containsEntry("sdh.compliance.waive.audit-export",
                "sidecar");
        // Under the profile, the KMS set this way is seen, and the waiver
        // names its check.
        settings.put(ComplianceRules.PROFILE, "hipaa");
        ComplianceRules.Outcome outcome = ComplianceRules.evaluate(settings,
                option -> Optional.of("/dev/null"));
        assertThat(outcome.refused()).extracting(f -> f.check().id())
                .doesNotContain("keys-local", "audit-export")
                .contains("transport-plaintext");
    }

    /** Profile none lets anything start. */
    @Test
    void profileNoneLetsItStart()
    {
        runner.withPropertyValues("sdh.compliance.profile=none",
                "sdh.crypto.vault.address=http://vault:8200")
                .run(context -> assertThat(context).hasNotFailed());
    }
}
