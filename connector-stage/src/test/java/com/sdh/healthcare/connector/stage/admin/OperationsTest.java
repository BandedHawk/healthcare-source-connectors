/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.stage.RegistryReport;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The operation layer (CONTROL-API-DESIGN.md §3, build step C1): each
 * shared command is an {@link Operation} that names the permission, kind
 * and approval the design's table gives it (§5), and runs from a request
 * with no CLI, its result printed by the CLI alone.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class OperationsTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    OperationsTest()
    {
    }

    /**
     * Each shared command names the permission and kind the design gives
     * it, and only audit prune needs a second person for its usual
     * request.
     */
    @Test
    void eachCommandNamesItsPermissionAndKind()
    {
        Map<String, String> found = new LinkedHashMap<>();
        List<Operation<?, ?, ?>> operations = new ArrayList<>();
        operations.add(new StageStatusCommand<>());
        operations.add(new StuckCommand<>());
        operations.add(new DrainCheckCommand<>());
        operations.add(new VersionCommand<>());
        operations.add(new ReprocessCommand<>());
        operations.add(new PatientsCommand<>());
        FailureCommands.all().forEach(each ->
                operations.add((Operation<?, ?, ?>) each));
        AuditCommands.all().forEach(each ->
                operations.add((Operation<?, ?, ?>) each));
        for (Operation<?, ?, ?> operation : operations)
        {
            found.put(operation.name(), operation.permission().key() + " "
                    + operation.kind());
        }

        assertThat(found).containsExactlyInAnyOrderEntriesOf(Map.ofEntries(
                Map.entry("status", "pipeline.view QUERY"),
                Map.entry("stuck", "pipeline.view QUERY"),
                Map.entry("drain-check", "pipeline.view QUERY"),
                Map.entry("failures", "pipeline.view QUERY"),
                Map.entry("version", "pipeline.operate ACTION"),
                Map.entry("decline", "pipeline.operate ACTION"),
                Map.entry("reopen", "pipeline.operate ACTION"),
                Map.entry("correct", "pipeline.operate ACTION"),
                Map.entry("retry", "pipeline.operate ACTION"),
                Map.entry("reprocess", "pipeline.operate LONG"),
                Map.entry("export", "phi.export SEALED"),
                Map.entry("patients", "phi.export SEALED"),
                Map.entry("audit-verify", "audit.read QUERY"),
                Map.entry("audit-export", "audit.read QUERY"),
                Map.entry("audit-prune", "audit.admin LONG")));
        AuditCommands.Prune prune = new AuditCommands.Prune();
        assertThat(prune.approval(new AuditCommands.Prune.Request(null)))
                .isEqualTo(Operation.Approval.FOUR_EYES);
        assertThat(new StuckCommand<>().approval(
                new StuckCommand.Request(Duration.ZERO)))
                .isEqualTo(Operation.Approval.NONE);
    }

    /**
     * A request asks for less, or more, than the usual permission: listing
     * releases or a run's status is a viewer's; a correction is an
     * operator's and PHI export's. Only queries still run when the audit
     * journal can't be written.
     */
    @Test
    void aRequestCanNeedAnotherPermission()
    {
        VersionCommand<DrainAdmin> version = new VersionCommand<>();
        assertThat(version.permissions(new VersionCommand.Request(false,
                null))).containsExactly(Permission.PIPELINE_VIEW);
        assertThat(version.permissions(new VersionCommand.Request(true,
                null))).containsExactly(Permission.PIPELINE_OPERATE);

        ReprocessCommand<ReprocessAdmin> reprocess = new ReprocessCommand<>();
        assertThat(reprocess.permissions(new ReprocessCommand.Request(true,
                false, UUID.randomUUID(), List.of(), null, null, null, null,
                null, false, null)))
                .containsExactly(Permission.PIPELINE_VIEW);

        FailureCommands.CorrectCommand<FailureAdmin> correct =
                new FailureCommands.CorrectCommand<>();
        assertThat(correct.permissions(new FailureCommands.CorrectCommand
                .Request(UUID.randomUUID(), 1, null)))
                .isEqualTo(EnumSet.of(Permission.PIPELINE_OPERATE,
                        Permission.PHI_EXPORT));

        assertThat(new StuckCommand<>().readsOnly()).isTrue();
        assertThat(version.readsOnly()).isFalse();
        assertThat(new PatientsCommand<>().readsOnly()).isFalse();
    }

    /**
     * An operation runs from a request, with no CLI: each part is read on
     * its own, a part that can't be read is kept as its failure, and the
     * audit event is the invocation's. The CLI prints the result as it
     * always has, the part unavailable, and fails.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void anOperationRunsWithoutTheCli() throws Exception
    {
        AuditRecord audit = new AuditRecord("alice");
        StuckCommand<StageAdmin> stuck = new StuckCommand<>();
        StuckCommand.Result result = stuck.run(
                new StuckCommand.Request(Duration.ofMinutes(5)),
                new Unreachable(), new Invocation(new Caller("alice",
                        Caller.Auth.JWT), audit, CliOperation.sink(
                                new PrintStream(new ByteArrayOutputStream(),
                                        true, UTF_8))));

        assertThat(result.idle()).isEqualTo(Duration.ofMinutes(5));
        assertThat(result.items().failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(result.claims().failure())
                .isInstanceOf(IllegalStateException.class);
        assertThat(audit.fields().toStrings()).containsEntry("sdh.stage",
                "test-stage");

        ByteArrayOutputStream printed = new ByteArrayOutputStream();
        assertThatThrownBy(() -> stuck.print(result,
                new PrintStream(printed, true, UTF_8)))
                .isInstanceOf(StatusParts.Unavailable.class);
        assertThat(printed.toString(UTF_8))
                .contains("stuck items:")
                .contains("unavailable (IllegalStateException");
    }

    /**
     * The CLI's sink prints rows as JSON lines and sealed parts after
     * their prefix.
     */
    @Test
    void theCliSinkPrintsRowsAndSealedParts()
    {
        ByteArrayOutputStream printed = new ByteArrayOutputStream();
        Invocation.Sink sink = CliOperation.sink(
                new PrintStream(printed, true, UTF_8));
        sink.row(Map.of("seq", 1));
        sink.sealed("a.b.c.d.e");

        assertThat(printed.toString(UTF_8).lines()).containsExactly(
                "{\"seq\":1}", ExportSealer.LINE_PREFIX + "a.b.c.d.e");
    }

    /** A stage whose registry can't be reached. */
    static final class Unreachable implements StageAdmin
    {

        /** Creates it. */
        Unreachable()
        {
        }

        @Override
        public String stage()
        {
            return "test-stage";
        }

        @Override
        public String connectorId()
        {
            return "test";
        }

        @Override
        public JdbcLeaseStore leases()
        {
            throw new IllegalStateException("no database");
        }

        @Override
        public RegistryReport registry()
        {
            throw new IllegalStateException("no database");
        }
    }
}
