/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.stage.control.ControlStore;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The control API's store (CONTROL-API-DESIGN.md §5, §8, build step C3):
 * idempotency keys and their answers, operation records and their pages,
 * and the leases a control pod holds on an operation it runs.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlStoreTest
{

    /** The scope the records here are of. */
    static final String SCOPE = "stage:test";

    /** The store, in a schema of its own. */
    ControlStore store;

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlStoreTest()
    {
    }

    /** Opens the store in a fresh schema. */
    @BeforeEach
    void open()
    {
        store = new ControlStore(StageTestSupport.database(),
                StageTestSupport.freshSchema());
    }

    /**
     * A new key is fresh; while its request runs, the same request is
     * pending; once answered, it is answered the same again; another
     * request with it is refused; another caller's key of the same text
     * is its own.
     */
    @Test
    void answersARetryWithTheFirstAnswer()
    {
        byte[] request = "POST /a\n{}".getBytes(UTF_8);
        byte[] other = "POST /a\n{\"x\":1}".getBytes(UTF_8);
        assertThat(store.claim("alice", "key-00001", "decline", request))
                .isInstanceOf(ControlStore.Fresh.class);
        assertThat(store.claim("alice", "key-00001", "decline", request))
                .isInstanceOf(ControlStore.Pending.class);

        store.answered("alice", "key-00001", 202, "application/json",
                "{\"id\":1}", "/api/v1/operations/1");
        assertThat(store.claim("alice", "key-00001", "decline", request))
                .isEqualTo(new ControlStore.Replay(202, "application/json",
                        "{\"id\":1}", "/api/v1/operations/1"));
        assertThat(store.claim("alice", "key-00001", "decline", other))
                .isInstanceOf(ControlStore.Mismatch.class);
        assertThat(store.claim("bob", "key-00001", "decline", other))
                .isInstanceOf(ControlStore.Fresh.class);
    }

    /**
     * Records are found by ID within their scope only, and listed newest
     * first a page at a time.
     */
    @Test
    void listsRecordsNewestFirstAPageAtATime()
    {
        ControlStore.Operation first = record();
        ControlStore.Operation second = record();
        ControlStore.Operation third = record();
        assertThat(store.find(first.id(), SCOPE)).hasValue(first);
        assertThat(store.find(first.id(), "stage:other")).isEmpty();
        assertThat(first.parameters()).containsEntry("runId", "r-1");
        assertThat(first.state()).isEqualTo(ControlStore.State.RUNNING);

        List<ControlStore.Operation> page = store.list(SCOPE, 2, null);
        assertThat(page).extracting(ControlStore.Operation::id)
                .containsExactly(third.id(), second.id());
        assertThat(store.list(SCOPE, 2, page.getLast()))
                .extracting(ControlStore.Operation::id)
                .containsExactly(first.id());
        assertThat(store.list("stage:other", 10, null)).isEmpty();
    }

    /** A record's progress changes until it has finished, then not. */
    @Test
    void keepsAFinishedRecordAsItFinished()
    {
        ControlStore.Operation running = record();
        assertThat(store.progress(running.id(), ControlStore.State.RUNNING,
                Map.of("done", 1))).isTrue();
        assertThat(store.progress(running.id(), ControlStore.State.DONE,
                Map.of("done", 2))).isTrue();
        assertThat(store.progress(running.id(), ControlStore.State.RUNNING,
                Map.of("done", 3))).isFalse();
        ControlStore.Operation done = store.find(running.id(), SCOPE)
                .orElseThrow();
        assertThat(done.state()).isEqualTo(ControlStore.State.DONE);
        assertThat(done.progress()).containsEntry("done", 2L);
    }

    /**
     * One control pod at a time holds an operation's lease: another can't
     * take it, renew it or give it up; once given up, or run out, another
     * may take it, and the first can't renew it. A finished operation
     * can't be taken.
     *
     * @throws InterruptedException if the wait is interrupted
     */
    @Test
    void letsOnePodAtATimeHoldAnOperation() throws InterruptedException
    {
        Duration minute = Duration.ofMinutes(1);
        UUID id = record().id();
        assertThat(store.take(id, "pod-a", minute)).isTrue();
        assertThat(store.take(id, "pod-a", minute)).isTrue();
        assertThat(store.take(id, "pod-b", minute)).isFalse();
        assertThat(store.renew(id, "pod-a", minute)).isTrue();
        assertThat(store.renew(id, "pod-b", minute)).isFalse();
        store.release(id, "pod-b");
        assertThat(store.take(id, "pod-b", minute)).isFalse();
        store.release(id, "pod-a");
        assertThat(store.take(id, "pod-b", Duration.ofMillis(1))).isTrue();
        assertThat(store.find(id, SCOPE).orElseThrow().holder())
                .isEqualTo("pod-b");

        Thread.sleep(50);
        assertThat(store.renew(id, "pod-b", minute)).isFalse();
        assertThat(store.take(id, "pod-a", minute)).isTrue();

        UUID done = record().id();
        store.progress(done, ControlStore.State.DONE, Map.of());
        assertThat(store.take(done, "pod-a", minute)).isFalse();
    }

    /**
     * Records a running reprocess.
     *
     * @return its record
     */
    ControlStore.Operation record()
    {
        return store.record("reprocess", SCOPE, Map.of("runId", "r-1"),
                "alice", "ticket 42", ControlStore.State.RUNNING, Map.of());
    }
}
