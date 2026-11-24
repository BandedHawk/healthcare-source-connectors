/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Runs {@link ItemCheckpointsTest}'s tests on a store that gathers the
 * writes and reads asked for at once (STAGES.md §18.3, part B), and tests
 * that each item's write in a gathered statement is fenced alone.
 */
class GatheredCheckpointsTest extends ItemCheckpointsTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    GatheredCheckpointsTest()
    {
    }

    /**
     * Makes a store that gathers up to 64 items a statement.
     *
     * @return it
     */
    @Override
    ItemCheckpoints newCheckpoints()
    {
        return new ItemCheckpoints(database, schema,
                new CheckpointCodec(new RecordIds(new byte[32])),
                Duration.ofSeconds(10), 64);
    }

    /**
     * Many items' writes at once, some under a stale epoch: those alone
     * are fenced, each others' is written, and each caller reads its own.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void eachItemInAGatheredStatementIsFencedAlone() throws Exception
    {
        int count = 120;
        List<UUID> items = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            UUID itemId = UUID.randomUUID();
            items.add(itemId);
            checkpoints.save(start(itemId), new Held(2));
        }
        try (ExecutorService callers =
                Executors.newVirtualThreadPerTaskExecutor())
        {
            List<Future<Boolean>> saves = new ArrayList<>();
            for (int i = 0; i < count; i++)
            {
                UUID itemId = items.get(i);
                long epoch = i % 7 == 0 ? 1 : 2;
                saves.add(callers.submit(() ->
                {
                    Checkpoint next = start(itemId)
                            .committed(ResumePoint.at(40), 3, -1);
                    try
                    {
                        checkpoints.save(next, new Held(epoch));
                        return true;
                    }
                    catch (LeaseLostException fenced)
                    {
                        return false;
                    }
                }));
            }
            for (int i = 0; i < count; i++)
            {
                assertThat(saves.get(i).get(30, TimeUnit.SECONDS))
                        .isEqualTo(i % 7 != 0);
            }
            List<Future<ItemCheckpoints.Stored>> loads = new ArrayList<>();
            for (UUID itemId : items)
            {
                loads.add(callers.submit(() ->
                        checkpoints.load(itemId).orElseThrow()));
            }
            for (int i = 0; i < count; i++)
            {
                ItemCheckpoints.Stored stored =
                        loads.get(i).get(30, TimeUnit.SECONDS);
                assertThat(stored.checkpoint().itemId())
                        .isEqualTo(items.get(i));
                assertThat(stored.checkpoint().resumeAt())
                        .isEqualTo(i % 7 == 0 ? start(items.get(i))
                                .resumeAt() : ResumePoint.at(40));
            }
        }
    }

    /**
     * Two pods' gathered statements on the same items at once, in reverse
     * orders, never deadlock: each statement locks its rows in item order.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void twoPodsWritingTheSameItemsAtOnceDontDeadlock() throws Exception
    {
        ItemCheckpoints other = newCheckpoints();
        int count = 48;
        List<UUID> items = new ArrayList<>();
        for (int i = 0; i < count; i++)
        {
            UUID itemId = UUID.randomUUID();
            items.add(itemId);
            checkpoints.save(start(itemId), new Held(1));
        }
        try (ExecutorService callers =
                Executors.newVirtualThreadPerTaskExecutor())
        {
            List<Future<?>> writes = new ArrayList<>();
            for (int round = 0; round < 20; round++)
            {
                for (int i = 0; i < count; i++)
                {
                    UUID mine = items.get(i);
                    UUID theirs = items.get(count - 1 - i);
                    writes.add(callers.submit(() -> checkpoints.save(
                            start(mine), new Held(1))));
                    writes.add(callers.submit(() -> other.save(
                            start(theirs), new Held(1))));
                }
            }
            for (Future<?> write : writes)
            {
                write.get(60, TimeUnit.SECONDS);
            }
        }
    }
}
