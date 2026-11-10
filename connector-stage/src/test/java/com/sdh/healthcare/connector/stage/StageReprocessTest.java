/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.broker.receive.BrokerMessage;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordReprocess;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordStored;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.core.failure.FailureReason;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * Tests for a stage's reprocess (STAGES.md §15.8.4): a run recorded, its
 * objects queued by the worker and sent to the stage itself, then taken
 * as any record at their new line.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StageReprocessTest
{

    /** The ID of the stage's own signing key. */
    static final String SELF = "self";

    /** The audit events. */
    @RegisterExtension
    final AuditCapture audit = new AuditCapture();

    /** Every record committed. */
    final List<String> committed =
            Collections.synchronizedList(new ArrayList<>());

    /** The stage's output. */
    final StageTestSupport.RecordingPublisher output =
            new StageTestSupport.RecordingPublisher();

    /** The stage's own input, which reprocess messages are sent to. */
    final StageTestSupport.RecordingPublisher self =
            new StageTestSupport.RecordingPublisher();

    /** The stage's input as the stage before stored it, by item. */
    final Map<UUID, List<ReprocessInputs.Input>> inputs =
            new ConcurrentHashMap<>();

    /** The item the records are from. */
    final UUID item = UUID.randomUUID();

    /** The registry's schema for this test. */
    String schema;

    /** The stage's runs. */
    ReprocessStore runs;

    /** The worker. */
    StageWorker<String> worker;

    /** Creates the tests; JUnit runs each test on a new instance. */
    StageReprocessTest()
    {
    }

    /**
     * Gives each test its own tables, and a worker that reprocesses.
     *
     * @throws JOSEException if the signer can't be made
     */
    @BeforeEach
    void worker() throws JOSEException
    {
        schema = StageTestSupport.freshSchema();
        StageRegistry registry = StageTestSupport.registry(schema, "pod-a");
        runs = new ReprocessStore(StageTestSupport.database(), schema,
                "convert", Duration.ofSeconds(10));
        worker = new StageWorker<>(
                new StageTestSupport.TestProcessor(1, committed), registry,
                opener(), StageTestSupport.stageSealer(), output,
                StageWorkerTest.SETTINGS, new StageWorker.Reprocessing(runs,
                        itemId -> inputs.getOrDefault(itemId, List.of()),
                        selfSealer(), self, SELF));
    }

    /** Closes the worker. */
    @AfterEach
    void close()
    {
        worker.close();
    }

    /**
     * Opens what the stage receives: from the stage before, and from
     * itself.
     *
     * @return the opener
     */
    static MessageOpener opener()
    {
        return new MessageOpener(
                Map.of("rcp", StageTestSupport.STAGE_RECIPIENT.getPrivate()),
                Map.of("sig", (ECPublicKey) StageTestSupport.UPSTREAM_SIGNER
                        .getPublic(),
                        SELF, (ECPublicKey) StageTestSupport.STAGE_SIGNER
                                .getPublic()));
    }

    /**
     * Seals to the stage itself, signed by it.
     *
     * @return the sealer
     * @throws JOSEException if the signer can't be made
     */
    static MessageSealer selfSealer() throws JOSEException
    {
        return new MessageSealer(new ECDSASigner((ECPrivateKey)
                StageTestSupport.STAGE_SIGNER.getPrivate()), SELF,
                StageTestSupport.STAGE_RECIPIENT.getPublic(), "rcp");
    }

    /**
     * A record as the stage before sent it, current in the input.
     *
     * @param seq its number
     * @return the record
     */
    RecordStored upstream(int seq)
    {
        return new RecordStored("rec-" + item + "-" + seq, item, seq,
                seq * 10L, 10, "csv", "application/json", "csv-row/1", 10, 1,
                StageTestSupport.REF);
    }

    /**
     * Stores an item's records as received, with the input holding them.
     *
     * @param records how many
     * @throws InterruptedException if interrupted
     */
    void received(int records) throws InterruptedException
    {
        List<BrokerMessage> batch = new ArrayList<>();
        List<ReprocessInputs.Input> current = new ArrayList<>();
        for (int seq = 0; seq < records; seq++)
        {
            RecordStored record = upstream(seq);
            batch.add(StageTestSupport.received(record));
            current.add(new ReprocessInputs.Input(new ReprocessStore.Chosen(
                    item, seq, Lineage.FIRST, record.recordId()), false,
                    record));
        }
        batch.add(StageTestSupport.total(item, records));
        worker.handle(batch);
        inputs.put(item, current);
    }

    /**
     * Records a run of some of the item's records.
     *
     * @param seqs their numbers
     * @return the run's ID
     */
    UUID run(int... seqs)
    {
        List<ReprocessStore.Chosen> chosen = new ArrayList<>();
        for (int seq : seqs)
        {
            chosen.add(new ReprocessStore.Chosen(item, seq, Lineage.FIRST,
                    "rec-" + item + "-" + seq));
        }
        return runs.record("--item-id " + item, "op", "v1",
                id -> FailureReason.seal(StageTestSupport.CORRECTION_KEYS,
                        StageTestSupport.CORRECTION_KEK, id, -2, 1,
                        "a fixed mapping"), chosen);
    }

    /**
     * The messages sent to the stage itself, as received.
     *
     * @return the batch
     */
    List<BrokerMessage> fromSelf()
    {
        List<BrokerMessage> batch = new ArrayList<>();
        synchronized (self.sent)
        {
            for (OutboundMessage message : self.sent)
            {
                batch.add(new BrokerMessage(message.body(),
                        message.messageId(), Map.of()));
            }
        }
        return batch;
    }

    /**
     * The record messages the stage sent on.
     *
     * @return them, in order
     */
    List<RecordStored> sentRecords()
    {
        return output.opened().stream()
                .filter(RecordStored.class::isInstance)
                .map(RecordStored.class::cast).toList();
    }

    /**
     * A run's object is queued with the next revision of its place, sent
     * to the stage itself, and taken as any record of that line: stored,
     * sent on saying it was reprocessed and what it is a revision of, and
     * noted done with whether its output is the same. The run is then
     * done.
     *
     * @throws Exception if the worker fails
     */
    @Test
    void reprocessesAnObjectAsTheNextRevisionOfItsPlace() throws Exception
    {
        received(3);
        UUID runId = run(1);
        worker.reprocess();

        assertThat(self.sent).hasSize(1);
        MessageOpener.Opened opened = opener().openSigned(
                self.sent.get(0).body());
        assertThat(opened.signerKid()).isEqualTo(SELF);
        RecordReprocess again = (RecordReprocess) opened.message();
        assertThat(again.runId()).isEqualTo(runId);
        Lineage next = Lineage.FIRST.withRevision(Lineage.CONVERT, 2);
        assertThat(again.record().lineage()).isEqualTo(next);
        assertThat(self.sent.get(0).key()).isEqualTo("rec-" + item + "-1");
        assertThat(audit.names()).contains("stage.reprocess_queued");

        worker.handle(fromSelf());
        RecordStored made = sentRecords().getLast();
        assertThat(made.lineage()).isEqualTo(next);
        assertThat(made.cause()).isEqualTo(new ConnectorMessage.Cause(
                ConnectorMessage.Cause.REPROCESS, runId,
                "rec-" + item + "-1"));
        assertThat(runs.objects(runId)).singleElement().satisfies(object ->
        {
            assertThat(object.state())
                    .isEqualTo(ReprocessStore.ObjectState.DONE);
            assertThat(object.outcome()).isEqualTo("stored");
            assertThat(object.line()).isEqualTo(next);
            assertThat(object.sameOutput()).isTrue();
        });
        assertThat(audit.attributes("stage.reprocessed"))
                .containsEntry("sdh.outcome", "stored")
                .containsEntry("sdh.same_output", "true");

        worker.reprocess();
        assertThat(runs.run(runId).orElseThrow().state())
                .isEqualTo(ReprocessStore.RunState.DONE);
        assertThat(audit.attributes("stage.reprocess_done"))
                .containsEntry("sdh.count", 1L);
        assertThat(self.sent).hasSize(1);
    }

    /**
     * An object whose place now holds something newer in the input is
     * refused, superseded; one no longer in the input, past retention;
     * neither is sent.
     */
    @Test
    void refusesWhatIsSupersededOrGone()
    {
        Lineage newer = Lineage.FIRST.withRevision(Lineage.SPLIT, 2);
        inputs.put(item, List.of(new ReprocessInputs.Input(
                new ReprocessStore.Chosen(item, 0, newer,
                        "rec-" + item + "-0b"), false, upstream(0)
                                .withLineage(newer, false))));
        UUID runId = run(0, 1);
        worker.reprocess();

        assertThat(self.sent).isEmpty();
        assertThat(runs.objects(runId)).extracting(
                ReprocessStore.Listed::reason).containsExactly(
                        ReprocessStore.SUPERSEDED,
                        ReprocessStore.PAST_RETENTION);
        assertThat(runs.run(runId).orElseThrow().state())
                .isEqualTo(ReprocessStore.RunState.DONE);
    }

    /**
     * A reprocess message is taken only when the stage signed it, and any
     * other type only when the stage before did.
     *
     * @throws Exception if the worker fails
     */
    @Test
    void takesAReprocessOnlyFromTheStageItself() throws Exception
    {
        received(2);
        RecordReprocess forged = new RecordReprocess(upstream(1).withLineage(
                Lineage.FIRST.withRevision(Lineage.CONVERT, 2), false),
                UUID.randomUUID());
        int before = sentRecords().size();
        worker.handle(List.of(StageTestSupport.received(forged)));
        worker.handle(List.of(new BrokerMessage(selfSealer().seal(
                upstream(5)), "m", Map.of())));

        assertThat(sentRecords()).hasSize(before);
        assertThat(audit.names()).filteredOn("stage.message_rejected"::equals)
                .hasSize(2);
    }

    /**
     * Objects not yet queued are cancelled, and a run with nothing queued
     * is cancelled with them; nothing of it is sent after.
     */
    @Test
    void cancelsWhatIsNotYetQueued()
    {
        UUID runId = run(0, 1);
        assertThat(runs.cancel(runId)).isEqualTo(2);
        worker.reprocess();

        assertThat(self.sent).isEmpty();
        assertThat(runs.run(runId).orElseThrow().state())
                .isEqualTo(ReprocessStore.RunState.CANCELLED);
    }

    /**
     * An object given its line but never sent, as when a pod stops between
     * the two, is sent again after the grace, under the same line, so its
     * message ID is the same.
     *
     * @throws Exception if the store fails
     */
    @Test
    void sendsAgainWhatWasQueuedButNotSent() throws Exception
    {
        received(2);
        UUID runId = run(1);
        List<ReprocessStore.Queued> queued = runs.queue(10,
                chosen -> ReprocessInputs.refusal(chosen, inputs.get(item)));
        assertThat(queued).hasSize(1);
        assertThat(runs.unsent(Duration.ZERO, 10)).singleElement()
                .isEqualTo(queued.get(0));

        StageWorker<String> later = new StageWorker<>(
                new StageTestSupport.TestProcessor(1, committed),
                StageTestSupport.registry(schema, "pod-b"), opener(),
                StageTestSupport.stageSealer(), output,
                new StageWorker.Settings(4, Duration.ofMillis(2),
                        Duration.ofMillis(1), Duration.ofMillis(50),
                        Duration.ofSeconds(5)),
                new StageWorker.Reprocessing(runs,
                        itemId -> inputs.getOrDefault(itemId, List.of()),
                        selfSealer(), self, SELF));
        try
        {
            Thread.sleep(20);
            later.reprocess();
        }
        finally
        {
            later.close();
        }
        assertThat(self.sent).singleElement().satisfies(message ->
                assertThat(message.messageId()).isEqualTo("rp-" + item
                        + "-rec-" + item + "-1" + queued.get(0).line()
                                .suffix()));
        assertThat(runs.unsent(Duration.ZERO, 10)).isEmpty();
        assertThat(runs.objects(runId).get(0).line())
                .isEqualTo(queued.get(0).line());
    }

    /**
     * A reprocess queued after a correction was submitted at the place
     * takes the revision after the correction's, so the two never share a
     * line.
     *
     * @throws MessageRejectedException never
     */
    @Test
    void takesTheRevisionAfterAPendingCorrection()
            throws MessageRejectedException
    {
        try (java.sql.Connection connection = StageTestSupport.database()
                .getConnection())
        {
            com.sdh.healthcare.connector.core.failure.Revisions.next(
                    connection, schema, "convert", item, 1, Lineage.FIRST, 1,
                    10);
        }
        catch (java.sql.SQLException failure)
        {
            throw new IllegalStateException(failure);
        }
        inputs.put(item, List.of(new ReprocessInputs.Input(
                new ReprocessStore.Chosen(item, 1, Lineage.FIRST,
                        "rec-" + item + "-1"), false, upstream(1))));
        UUID runId = run(1);
        worker.reprocess();

        assertThat(runs.objects(runId).get(0).line()).isEqualTo(
                Lineage.FIRST.withRevision(Lineage.CONVERT, 3));
        assertThat(((RecordReprocess) opener().open(self.sent.get(0).body()))
                .record().lineage()).isEqualTo(
                        Lineage.FIRST.withRevision(Lineage.CONVERT, 3));
    }
}
