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
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.contract.message.MessageType;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.ReprocessAdmin;
import com.sdh.healthcare.connector.stage.admin.ReprocessCommand;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for the {@code reprocess} command ({@link ReprocessCommand}), run
 * on a context of the test's own over a real Postgres.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ReprocessCommandTest
{

    /** A reason, synthetic. */
    static final String REASON = "Synthetic reprocess reason 41c2";

    /** The audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** The item chosen. */
    final UUID item = UUID.randomUUID();

    /** The stage's input: three pieces, the last failed. */
    final List<ReprocessInputs.Input> pieces = new ArrayList<>();

    /** The registry's schema for this test. */
    String schema;

    /** What the command runs on. */
    Context context;

    /** Creates the tests; JUnit runs each test on a new instance. */
    ReprocessCommandTest()
    {
    }

    /** What the command uses: the test's tables and input. */
    final class Context implements ReprocessAdmin
    {

        /** The stage's runs. */
        final ReprocessStore runs = new ReprocessStore(
                StageTestSupport.database(), schema, "convert",
                Duration.ofSeconds(10));

        /** What the command reads as its standard input. */
        byte[] input = REASON.getBytes(UTF_8);

        /** Whether it reprocesses whole items, as the split stage does. */
        boolean whole;

        /** The landing times asked for, as {@code [from, to]}. */
        final List<Instant> landedAsked = new ArrayList<>();

        /** Creates the context. */
        Context()
        {
        }

        @Override
        public String stage()
        {
            return "convert";
        }

        @Override
        public FailureStore failures()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FailureStore failuresToChange()
        {
            throw new UnsupportedOperationException();
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
            return new ByteArrayInputStream(input);
        }

        @Override
        public void changed(UUID itemId, AuditRecord record, PrintStream out)
        {
        }

        @Override
        public ReprocessStore reprocessRuns()
        {
            return runs;
        }

        @Override
        public ReprocessStore reprocessRunsToChange()
        {
            return runs;
        }

        @Override
        public ReprocessInputs reprocessInputs()
        {
            return itemId -> itemId.equals(item) ? pieces : List.of();
        }

        @Override
        public String version()
        {
            return "v2";
        }

        @Override
        public boolean wholeItems()
        {
            return whole;
        }

        /** The test's item, as landed in any range, at a split stage. */
        @Override
        public List<UUID> landedBetween(String intake, Instant from,
                Instant to, int limit) throws Exception
        {
            if (!whole)
            {
                return ReprocessAdmin.super.landedBetween(intake, from, to,
                        limit);
            }
            landedAsked.add(from);
            landedAsked.add(to);
            return List.of(item);
        }
    }

    /** Gives each test its own tables and input. */
    @BeforeEach
    void tables()
    {
        schema = StageTestSupport.freshSchema();
        StageTestSupport.registry(schema, "pod-a");
        context = new Context();
        for (int seq = 0; seq < 3; seq++)
        {
            pieces.add(new ReprocessInputs.Input(new ReprocessStore.Chosen(
                    item, seq, Lineage.FIRST, "rec-" + seq), seq == 2,
                    null));
        }
    }

    /**
     * Runs the command.
     *
     * @param args its arguments, after its name
     * @return what it printed
     * @throws Exception if it fails
     */
    String run(String... args) throws Exception
    {
        List<String> all = new ArrayList<>(List.of("reprocess"));
        all.addAll(List.of(args));
        ReprocessCommand<Context> command = new ReprocessCommand<>();
        AdminArgs parsed = AdminArgs.parse(all, command.flags());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        command.run(parsed, context, new AuditRecord("alice"),
                new PrintStream(out, true, UTF_8));
        return out.toString(UTF_8);
    }

    /**
     * A dry run lists what an item would have reprocessed, its failed
     * piece left out, and records nothing.
     *
     * @throws Exception if it fails
     */
    @Test
    void aDryRunRecordsNothing() throws Exception
    {
        String printed = run("--item-id", item.toString(), "--dry-run");

        assertThat(printed).contains("Would reprocess 2 object(s)")
                .contains(item + " seq 0 g1 r1")
                .contains(item + " seq 1 g1 r1")
                .doesNotContain("seq 2");
        assertThat(context.runs.queue(10, chosen -> null)).isEmpty();
    }

    /**
     * A run is recorded with its objects, its reason kept encrypted, and
     * listed by {@code --status}; {@code --cancel} cancels what isn't yet
     * queued.
     *
     * @throws Exception if it fails
     */
    @Test
    void recordsListsAndCancelsARun() throws Exception
    {
        String printed = run("--item-id", item.toString(), "--seq", "1");
        UUID runId = UUID.fromString(printed.substring(4, 40));

        assertThat(context.runs.run(runId).orElseThrow()).satisfies(run ->
        {
            assertThat(run.selector()).isEqualTo("--item-id " + item
                    + " --seq 1");
            assertThat(run.operator()).isEqualTo("alice");
            assertThat(run.version()).isEqualTo("v2");
            assertThat(run.state())
                    .isEqualTo(ReprocessStore.RunState.PENDING);
        });
        assertThat(context.runs.objects(runId)).extracting(
                listed -> listed.chosen().seq()).containsExactly(1L);
        assertThat(run("--status", "--run", runId.toString()))
                .contains("PENDING").contains(item + " seq 1 g1 r1 PENDING")
                .doesNotContain(REASON);
        assertThat(run("--cancel", "--run", runId.toString()))
                .contains("1 object(s) not yet queued cancelled");
        assertThat(context.runs.run(runId).orElseThrow().state())
                .isEqualTo(ReprocessStore.RunState.CANCELLED);
    }

    /**
     * {@code --made-by} chooses the places whose current output here was
     * made by that version.
     *
     * @throws Exception if it fails
     */
    @Test
    void choosesWhatAVersionMade() throws Exception
    {
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        for (int seq = 0; seq < 2; seq++)
        {
            registry.claim("rec-" + seq, item, Duration.ofMinutes(1));
            registry.store(List.of(new StageRegistry.Entry("rec-" + seq,
                    item, "d", seq == 0 ? "v1" : "v2", false,
                    new OutboundMessage("m-" + seq, MessageType.RECORD_STORED,
                            item, "out-" + seq, new byte[] {1}), null, null,
                    seq, Lineage.FIRST)));
        }

        assertThat(run("--made-by", "v1", "--dry-run"))
                .contains("Would reprocess 1 object(s)")
                .contains(item + " seq 0 g1 r1");
    }

    /**
     * At a stage that reprocesses whole items, {@code --seq} is refused,
     * and items are chosen by landing time too.
     *
     * @throws Exception if it fails
     */
    @Test
    void choosesWholeItemsByLandingTime() throws Exception
    {
        context.whole = true;
        pieces.clear();
        pieces.add(new ReprocessInputs.Input(new ReprocessStore.Chosen(item,
                -1, Lineage.FIRST, item.toString()), false, null));

        assertThatThrownBy(() -> run("--item-id", item.toString(), "--seq",
                "1")).isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("whole item");
        assertThat(run("--landed-from", "2026-11-01T00:00:00Z",
                "--landed-to", "2026-11-02T00:00:00+01:00", "--dry-run"))
                .contains("Would reprocess 1 object(s)")
                .contains("--landed-from 2026-11-01T00:00:00Z")
                .contains(item + " seq -1 g1");
        assertThat(context.landedAsked).containsExactly(
                Instant.parse("2026-11-01T00:00:00Z"),
                Instant.parse("2026-11-01T23:00:00Z"));
    }

    /**
     * Landing time is the split stage's only, and given whole.
     */
    @Test
    void refusesLandingTimeOutOfPlace()
    {
        assertThatThrownBy(() -> run("--landed-from",
                "2026-11-01T00:00:00Z", "--landed-to",
                "2026-11-02T00:00:00Z", "--dry-run"))
                .isInstanceOf(AdminArgs.UsageException.class)
                .hasMessageContaining("split stage only");
        context.whole = true;
        assertThatThrownBy(() -> run("--landed-from",
                "2026-11-01T00:00:00Z", "--dry-run"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run("--landed-from",
                "2026-11-02T00:00:00Z", "--landed-to",
                "2026-11-01T00:00:00Z", "--dry-run"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run("--landed-from", "yesterday",
                "--landed-to", "2026-11-01T00:00:00Z", "--dry-run"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run("--made-by", "v1", "--landed-from",
                "2026-11-01T00:00:00Z", "--landed-to",
                "2026-11-02T00:00:00Z"))
                .isInstanceOf(AdminArgs.UsageException.class);
    }

    /** Mistakes in the choice are usage errors. */
    @Test
    void refusesAnUnclearChoice()
    {
        assertThatThrownBy(() -> run("--dry-run"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run("--item-id", item + ","
                + UUID.randomUUID(), "--seq", "1"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run("--status"))
                .isInstanceOf(AdminArgs.UsageException.class);
        assertThatThrownBy(() -> run("--made-by", "a b"))
                .isInstanceOf(AdminArgs.UsageException.class);
    }
}
