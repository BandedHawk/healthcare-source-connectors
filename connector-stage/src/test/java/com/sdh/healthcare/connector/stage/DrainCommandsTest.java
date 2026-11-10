/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AdminCommand;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.DrainAdmin;
import com.sdh.healthcare.connector.stage.admin.DrainCheckCommand;
import com.sdh.healthcare.connector.stage.admin.VersionCommand;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for {@code drain-check} and {@code version}
 * ({@link DrainCheckCommand}, {@link VersionCommand}), run on a context of
 * the test's own over a real Postgres.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class DrainCommandsTest
{

    /** A reason, synthetic. */
    static final String REASON = "Synthetic acceptance reason 7d0e";

    /** The audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** The items part way, as the test sets them. */
    final List<PartWay.Items> parts = new ArrayList<>();

    /** The registry's schema for this test. */
    String schema;

    /** What the commands run on. */
    Context context;

    /** Creates the tests; JUnit runs each test on a new instance. */
    DrainCommandsTest()
    {
    }

    /** What the commands use: the test's tables. */
    final class Context implements DrainAdmin
    {

        /** The stage's releases. */
        final StageReleases releases = new StageReleases(
                StageTestSupport.database(), schema, "convert",
                Duration.ofSeconds(10));

        /** The versions this release stamps. */
        List<String> versions = List.of("v2", "none");

        /** Creates the context. */
        Context()
        {
            releases.createTablesIfMissing();
        }

        @Override
        public String stage()
        {
            return "convert";
        }

        @Override
        public FailureStore failures()
        {
            return new FailureStore(StageTestSupport.database(), schema,
                    "convert", Duration.ofSeconds(10));
        }

        @Override
        public FailureStore failuresToChange()
        {
            return failures();
        }

        @Override
        public KeyProvider reasonKeys()
        {
            return StageTestSupport.CORRECTION_KEYS;
        }

        @Override
        public String reasonKek()
        {
            return StageTestSupport.CORRECTION_KEK;
        }

        @Override
        public InputStream reasonInput()
        {
            return new ByteArrayInputStream(REASON.getBytes(UTF_8));
        }

        @Override
        public void changed(UUID itemId, AuditRecord record, PrintStream out)
        {
        }

        @Override
        public ReprocessStore reprocessRuns()
        {
            return new ReprocessStore(StageTestSupport.database(), schema,
                    "convert", Duration.ofSeconds(10));
        }

        @Override
        public ReprocessStore reprocessRunsToChange()
        {
            return reprocessRuns();
        }

        @Override
        public ReprocessInputs reprocessInputs()
        {
            return itemId -> List.of();
        }

        @Override
        public String version()
        {
            return "v2";
        }

        @Override
        public Collection<String> releaseVersions()
        {
            return versions;
        }

        @Override
        public StageReleases stageReleases()
        {
            return releases;
        }

        @Override
        public StageReleases stageReleasesToChange()
        {
            return releases;
        }

        @Override
        public PartWay partWay()
        {
            return first -> List.copyOf(parts);
        }
    }

    /** Gives each test its own tables. */
    @BeforeEach
    void tables()
    {
        schema = StageTestSupport.freshSchema();
        StageTestSupport.registry(schema, "pod-a");
        context = new Context();
        context.releases.letIn(StageReleases.Release.of(List.of("v1",
                "none")), "first");
    }

    /**
     * Runs a command.
     *
     * @param command the command
     * @param args    its arguments, after its name
     * @return what it printed
     * @throws Exception if it fails
     */
    String run(AdminCommand<Context> command, String... args)
            throws Exception
    {
        List<String> all = new ArrayList<>(List.of(command.name()));
        all.addAll(List.of(args));
        AdminArgs parsed = AdminArgs.parse(all, command.flags());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        command.run(parsed, context, new AuditRecord("alice"),
                new PrintStream(out, true, UTF_8));
        return out.toString(UTF_8);
    }

    /**
     * {@code drain-check} says what holds a release back, and lists what
     * waits on an operator apart; it keeps nothing.
     *
     * @throws Exception if it fails
     */
    @Test
    void drainCheckSaysWhatHoldsARelease() throws Exception
    {
        UUID item = UUID.randomUUID();
        parts.add(new PartWay.Items("v1", 1, List.of(item)));
        String printed = run(new DrainCheckCommand<>());
        assertThat(printed)
                .contains("this release "
                        + StageReleases.Release.of(context.versions).digest())
                .contains("Held back: 1 item(s) part way under 1 version(s)")
                .contains("1 under v1: " + item)
                .contains("no open failures")
                .contains("0 reprocess object(s) waiting")
                .contains("Previous: none");
        assertThat(context.releases.history(10)).hasSize(1);

        parts.clear();
        assertThat(run(new DrainCheckCommand<>()))
                .contains("Would be let in: drained")
                .contains("Part way:\n  none");
    }

    /**
     * {@code version} lists the releases; {@code --accept} lets a held one
     * in, with its reason kept encrypted, and refuses the current one.
     *
     * @throws Exception if it fails
     */
    @Test
    void versionAcceptsAHeldRelease() throws Exception
    {
        parts.add(new PartWay.Items("v1", 2, List.of(UUID.randomUUID())));
        assertThat(run(new VersionCommand<>()))
                .contains("This release: ")
                .contains("let in first at");

        assertThat(run(new VersionCommand<>(), "--accept"))
                .contains("accepted");
        assertThat(audit.names()).contains("stage.version_forced");
        assertThat(audit.attributes("stage.version_forced"))
                .containsEntry("sdh.operator", "alice")
                .doesNotContainValue(REASON);
        StageReleases.Release mine =
                StageReleases.Release.of(context.versions);
        assertThat(context.releases.accepted(mine)).get()
                .extracting(StageReleases.Accepted::operator)
                .isEqualTo("alice");
        assertThat(run(new DrainCheckCommand<>()))
                .contains("Would be let in: forced");
        assertThat(run(new VersionCommand<>()))
                .contains("This release was accepted by alice");

        context.versions = List.of("v1", "none");
        assertThatThrownBy(() -> run(new VersionCommand<>(), "--accept"))
                .isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("current one already");
    }
}
