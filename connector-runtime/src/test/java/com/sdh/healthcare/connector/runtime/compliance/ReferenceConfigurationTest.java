/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileSystemResource;

/**
 * The reference settings in {@code deploy/reference} (HIPAA.md §8.1 item
 * 13): under the {@code hipaa} profile, as each node applies it at startup
 * (its settings read from the environment by {@link
 * ComplianceConfiguration}, its JVM options those its image starts with),
 * nothing is refused and nothing waived. So too the AWS PoC's settings,
 * which turn the profile on since its DESIGN.md revision (fk), so that its
 * runs measure what a production deployment does. The sandbox's settings,
 * which turn it off, are refused when it is on, so the check isn't empty.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ReferenceConfigurationTest
{

    /** The repository's root: the tests run in the module's directory. */
    static final Path ROOT = Path.of("").toAbsolutePath().getParent();

    /** Creates the tests; JUnit runs each test on a new instance. */
    ReferenceConfigurationTest()
    {
    }

    /**
     * Each stage's reference settings pass the profile, with its image's
     * JVM options.
     *
     * @param stage the settings' file, before {@code -application.yml}
     * @param dist  the stage's distribution, from the repository's root
     * @throws IOException if a file can't be read
     */
    @ParameterizedTest
    @CsvSource({
        "landing, landing-connector/src/dist",
        "splitter, file-connector/src/dist",
        "convert, convert-stage/src/dist",
        "transform, ../healthcare-data-transformer/transform-stage/src/dist"})
    void theProfileRefusesNothingInTheReferenceSettings(String stage,
            String dist) throws IOException
    {
        Path options = ROOT.resolve(dist);
        assumeTrue(Files.isDirectory(options), "the transformer's checkout,"
                + " beside this one, for its image's JVM options");
        ComplianceRules.Outcome outcome = ComplianceRules.evaluate(
                settings(ROOT.resolve("deploy/reference/" + stage
                        + "-application.yml"), Map.of()),
                jvmOptions(options));

        assertThat(outcome.profile()).isEqualTo(ComplianceProfile.HIPAA);
        assertThat(outcome.refused()).as("refused").isEmpty();
        assertThat(outcome.waived()).as("waived").isEmpty();
    }

    /**
     * Each stage's AWS PoC settings, as apply.sh fills them in, pass the
     * profile, with its image's JVM options: Kafka over SASL_SSL, Postgres
     * with verify-full, nothing waived.
     *
     * @param stage the settings' file, before {@code -application.yml}
     * @param dist  the stage's distribution, from the repository's root
     * @throws IOException if a file can't be read
     */
    @ParameterizedTest
    @CsvSource({
        "landing, landing-connector/src/dist",
        "splitter, file-connector/src/dist",
        "convert, convert-stage/src/dist",
        "transform, ../healthcare-data-transformer/transform-stage/src/dist"})
    void theProfileRefusesNothingInThePocsSettings(String stage, String dist)
            throws IOException
    {
        Path options = ROOT.resolve(dist);
        assumeTrue(Files.isDirectory(options), "the transformer's checkout,"
                + " beside this one, for its image's JVM options");
        ComplianceRules.Outcome outcome = ComplianceRules.evaluate(
                settings(filled("deploy/aws/k8s/pipeline/" + stage
                        + "-application.yml"), Map.of()),
                jvmOptions(options));

        assertThat(outcome.profile()).isEqualTo(ComplianceProfile.HIPAA);
        assertThat(outcome.refused()).as("refused").isEmpty();
        assertThat(outcome.waived()).as("waived").isEmpty();
    }

    /**
     * The sandbox's settings, with the profile turned on, are refused: its
     * Kafka and Postgres are plaintext.
     *
     * @throws IOException if a file can't be read
     */
    @Test
    void theSandboxsSettingsAreRefusedWithTheProfileOn() throws IOException
    {
        ComplianceRules.Outcome outcome = ComplianceRules.evaluate(
                settings(filled("deploy/sandbox/pipeline/"
                        + "splitter-application.yml"),
                        Map.of(ComplianceRules.PROFILE, "hipaa")),
                jvmOptions(ROOT.resolve("file-connector/src/dist")));

        assertThat(outcome.refused()).extracting(ComplianceRules.Finding::check)
                .contains(ComplianceCheck.TRANSPORT_BROKER,
                        ComplianceCheck.TRANSPORT_DATABASE);
    }

    /**
     * A deployment script's settings file, with its {@code ${...}} in
     * capitals, which the script fills in before it is YAML, set to
     * {@code x}.
     *
     * @param file the file, from the repository's root
     * @return a temporary copy, filled in
     * @throws IOException if the file can't be read or the copy written
     */
    static Path filled(String file) throws IOException
    {
        String text = Files.readString(ROOT.resolve(file))
                .replaceAll("\\$\\{([A-Z][A-Z0-9_]*)\\}", "x");
        return Files.writeString(Files.createTempFile("settings", ".yml"),
                text);
    }

    /**
     * A node's settings, as it reads them at startup, from a settings file
     * and, above it, any overrides.
     *
     * @param file      the settings file
     * @param overrides settings that take precedence
     * @return the settings
     * @throws IOException if the file can't be read
     */
    static Map<String, String> settings(Path file,
            Map<String, Object> overrides) throws IOException
    {
        StandardEnvironment environment = new StandardEnvironment();
        MutablePropertySources sources = environment.getPropertySources();
        // Only the file: this machine's environment isn't the node's.
        sources.remove(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        sources.remove(
                StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        sources.addFirst(new MapPropertySource("overrides",
                new HashMap<>(overrides)));
        new YamlPropertySourceLoader().load(file.getFileName().toString(),
                new FileSystemResource(file)).forEach(sources::addLast);
        return ComplianceConfiguration.settings(environment);
    }

    /**
     * The JVM options an image starts its node with: {@code jvm.options},
     * then {@code container.options}, the later winning.
     *
     * @param dist the stage's distribution directory
     * @return each {@code -XX} option's value by name
     * @throws IOException if a file can't be read
     */
    static Function<String, Optional<String>> jvmOptions(Path dist)
            throws IOException
    {
        Map<String, String> options = new HashMap<>();
        for (Path file : List.of(dist.resolve("config/jvm.options"),
                dist.resolve("container/container.options")))
        {
            for (String line : Files.readAllLines(file))
            {
                String option = line.strip();
                if (!option.startsWith("-XX:"))
                {
                    continue;
                }
                String name = option.substring(4);
                int equals = name.indexOf('=');
                if (equals > 0)
                {
                    options.put(name.substring(0, equals),
                            name.substring(equals + 1));
                }
                else if (name.startsWith("+") || name.startsWith("-"))
                {
                    options.put(name.substring(1),
                            String.valueOf(name.startsWith("+")));
                }
            }
        }
        return name -> Optional.ofNullable(options.get(name));
    }
}
