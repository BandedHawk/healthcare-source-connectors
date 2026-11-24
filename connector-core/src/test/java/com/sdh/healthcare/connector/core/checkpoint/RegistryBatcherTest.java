/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link RegistryBatcher}: an input alone runs at once; inputs that
 * come while a statement runs go in the next together, one of each key, up
 * to the limit; each caller gets its own output, or the statement's
 * failure.
 */
class RegistryBatcherTest
{

    /** The callers. */
    private final ExecutorService callers =
            Executors.newVirtualThreadPerTaskExecutor();

    /** The statements run, each as the inputs it took. */
    private final List<List<String>> statements =
            Collections.synchronizedList(new ArrayList<>());

    /** Stops the callers. */
    @AfterEach
    void tearDown()
    {
        callers.shutdownNow();
    }

    /**
     * A statement that notes its inputs, then answers each with its upper
     * case, after the gate opens.
     *
     * @param gate held shut to make callers wait
     * @return it
     */
    private RegistryBatcher.Statement<String, String> upper(
            CountDownLatch gate)
    {
        return inputs ->
        {
            statements.add(List.copyOf(inputs));
            gate.await(10, TimeUnit.SECONDS);
            return inputs.stream().map(String::toUpperCase).toList();
        };
    }

    /**
     * Waits until so many statements have started.
     *
     * @param count how many
     * @throws InterruptedException if interrupted
     */
    private void awaitStatements(int count) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (statements.size() < count && System.nanoTime() < deadline)
        {
            Thread.sleep(5);
        }
        assertThat(statements).hasSizeGreaterThanOrEqualTo(count);
    }

    /**
     * An input with none waiting runs at once, alone, in its caller's
     * thread.
     */
    @Test
    void anInputAloneRunsAtOnce()
    {
        RegistryBatcher<String, String> batcher = new RegistryBatcher<>(64,
                Function.identity(), upper(new CountDownLatch(0)));
        Thread caller = Thread.currentThread();
        List<Thread> ran = new ArrayList<>();
        RegistryBatcher<String, String> seeing = new RegistryBatcher<>(64,
                Function.identity(), inputs ->
                {
                    ran.add(Thread.currentThread());
                    return inputs;
                });
        assertThat(batcher.run("a")).isEqualTo("A");
        assertThat(statements).containsExactly(List.of("a"));
        assertThat(seeing.run("b")).isEqualTo("b");
        assertThat(ran).containsExactly(caller);
    }

    /**
     * Inputs that come while a statement runs go in the next one together,
     * and each caller gets its own output.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void inputsThatComeWhileOneRunsGoTogether() throws Exception
    {
        CountDownLatch gate = new CountDownLatch(1);
        RegistryBatcher<String, String> batcher = new RegistryBatcher<>(64,
                Function.identity(), upper(gate));
        Future<String> first = callers.submit(() -> batcher.run("first"));
        awaitStatements(1);
        List<Future<String>> later = new ArrayList<>();
        for (int i = 0; i < 10; i++)
        {
            String input = "later-" + i;
            later.add(callers.submit(() -> batcher.run(input)));
        }
        // Let them all queue behind the first.
        Thread.sleep(200);
        gate.countDown();
        assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo("FIRST");
        for (int i = 0; i < 10; i++)
        {
            assertThat(later.get(i).get(10, TimeUnit.SECONDS))
                    .isEqualTo("LATER-" + i);
        }
        assertThat(statements).hasSize(2);
        assertThat(statements.get(1)).hasSize(10);
    }

    /**
     * A statement takes no more than the limit, and one input of a key.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aStatementTakesUpToTheLimitAndOneOfAKey() throws Exception
    {
        CountDownLatch gate = new CountDownLatch(1);
        RegistryBatcher<String, String> batcher = new RegistryBatcher<>(3,
                input -> input.substring(0, 1), upper(gate));
        Future<String> first = callers.submit(() -> batcher.run("z0"));
        awaitStatements(1);
        List<Future<String>> later = new ArrayList<>();
        for (String input : List.of("a1", "a2", "b1", "c1", "d1"))
        {
            later.add(callers.submit(() -> batcher.run(input)));
            // In this order.
            Thread.sleep(20);
        }
        gate.countDown();
        first.get(10, TimeUnit.SECONDS);
        for (Future<String> each : later)
        {
            each.get(10, TimeUnit.SECONDS);
        }
        assertThat(statements).containsExactly(List.of("z0"),
                List.of("a1", "b1", "c1"), List.of("a2", "d1"));
    }

    /**
     * A failed statement's failure goes to every caller in it, as it was
     * thrown, or wrapped when checked; the next statement runs.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void aFailureGoesToEveryCallerInTheStatement() throws Exception
    {
        IllegalStateException down = new IllegalStateException("down");
        RegistryBatcher<String, String> failing = new RegistryBatcher<>(64,
                Function.identity(), inputs ->
                {
                    throw down;
                });
        assertThatThrownBy(() -> failing.run("a")).isSameAs(down);
        RegistryBatcher<String, String> checked = new RegistryBatcher<>(64,
                Function.identity(), inputs ->
                {
                    throw new SQLException("checked");
                });
        assertThatThrownBy(() -> checked.run("a"))
                .isInstanceOf(IllegalStateException.class)
                .hasCauseInstanceOf(SQLException.class);
        CountDownLatch gate = new CountDownLatch(1);
        List<Integer> calls = Collections.synchronizedList(new ArrayList<>());
        RegistryBatcher<String, String> second = new RegistryBatcher<>(64,
                Function.identity(), inputs ->
                {
                    calls.add(inputs.size());
                    if (calls.size() == 1)
                    {
                        gate.await(10, TimeUnit.SECONDS);
                        return inputs;
                    }
                    if (calls.size() == 2)
                    {
                        throw down;
                    }
                    return inputs;
                });
        Future<String> first = callers.submit(() -> second.run("first"));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (calls.isEmpty() && System.nanoTime() < deadline)
        {
            Thread.sleep(5);
        }
        Future<String> a = callers.submit(() -> second.run("a"));
        Future<String> b = callers.submit(() -> second.run("b"));
        Thread.sleep(200);
        gate.countDown();
        assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo("first");
        for (Future<String> each : List.of(a, b))
        {
            assertThatThrownBy(() -> each.get(10, TimeUnit.SECONDS))
                    .hasCause(down);
        }
        assertThat(second.run("after")).isEqualTo("after");
        assertThat(calls).containsExactly(1, 2, 1);
    }

    /**
     * A statement giving the wrong number of outputs fails its callers.
     */
    @Test
    void theWrongNumberOfOutputsFails()
    {
        RegistryBatcher<String, String> wrong = new RegistryBatcher<>(64,
                Function.identity(), inputs -> List.of());
        assertThatThrownBy(() -> wrong.run("a"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("0 outputs for 1");
    }

    /**
     * Many callers at once, each getting its own output, in fewer
     * statements than callers.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void manyCallersEachGetTheirOwn() throws Exception
    {
        RegistryBatcher<String, String> batcher = new RegistryBatcher<>(16,
                Function.identity(), inputs ->
                {
                    statements.add(List.copyOf(inputs));
                    Thread.sleep(2);
                    return inputs.stream().map(String::toUpperCase).toList();
                });
        List<Future<String>> all = new ArrayList<>();
        for (int i = 0; i < 500; i++)
        {
            String input = "x" + i;
            all.add(callers.submit(() -> batcher.run(input)));
        }
        for (int i = 0; i < 500; i++)
        {
            assertThat(all.get(i).get(30, TimeUnit.SECONDS))
                    .isEqualTo("X" + i);
        }
        assertThat(statements.stream().mapToInt(List::size).sum())
                .isEqualTo(500);
        assertThat(statements).allSatisfy(statement ->
                assertThat(statement).hasSizeLessThanOrEqualTo(16));
        assertThat(statements.size()).isLessThan(500);
    }

    /** A limit under 1 is refused. */
    @Test
    void aLimitUnderOneIsRefused()
    {
        assertThatThrownBy(() -> new RegistryBatcher<String, String>(0,
                Function.identity(), inputs -> inputs))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
