/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.checkpoint;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Gathers one kind of statement from many callers into one statement, as a
 * group commit does (STAGES.md §18.3, part B).
 * <p>
 * A caller hands its input and waits for its output. If no statement of
 * this kind is running, the caller runs one at once, with what is waiting:
 * at low load each input runs alone, with no added wait. Inputs that come
 * while a statement runs wait, and the next statement takes them together,
 * up to the limit. There is no linger: batches grow only when callers
 * would have waited anyway.
 * <p>
 * A statement takes at most one input of each key (an item, a lease), so
 * no statement writes a row twice; another input of the same key waits for
 * the next. The statement is one transaction: its outputs are given each
 * to its caller, or, if it fails, its failure to every caller in it. Per-row
 * results, such as a row a fence left unwritten, are outputs, so they are
 * the caller's alone.
 *
 * @param <I> what a caller gives
 * @param <O> what the caller is given back
 */
public final class RegistryBatcher<I, O>
{

    /**
     * Runs one statement for many inputs.
     *
     * @param <I> what a caller gives
     * @param <O> what the caller is given back
     */
    @FunctionalInterface
    public interface Statement<I, O>
    {

        /**
         * Runs the statement.
         *
         * @param inputs the inputs, each of a different key
         * @return one output for each input, in the same order
         * @throws Exception if the statement fails; every caller in it is
         *                   given the failure
         */
        List<O> run(List<I> inputs) throws Exception;
    }

    /**
     * One caller's input, and what came of it.
     *
     * @param <I> what a caller gives
     * @param <O> what the caller is given back
     */
    private static final class Request<I, O>
    {

        /** The input. */
        private final I input;

        /** Signalled when the request is done, or it is its turn to run. */
        private final Condition ready;

        /** The output, once done. */
        private O output;

        /** The failure, if the statement failed. */
        private Throwable failure;

        /** Whether the request is done. */
        private boolean done;

        /** Whether its caller is to run the next statements. */
        private boolean turn;

        /**
         * Makes a request.
         *
         * @param input its input
         * @param ready signalled when it is done or its turn comes
         */
        private Request(I input, Condition ready)
        {
            this.input = input;
            this.ready = ready;
        }
    }

    /** The most inputs a statement takes. */
    private final int max;

    /** Each input's key: a statement takes one input of each. */
    private final Function<? super I, ?> key;

    /** The statement. */
    private final Statement<I, O> statement;

    /** Guards the queue and the running flag. */
    private final ReentrantLock lock = new ReentrantLock();

    /** The requests not yet run, in the order they came. */
    private final ArrayDeque<Request<I, O>> queue = new ArrayDeque<>();

    /** Whether a caller is running statements. */
    private boolean running;

    /**
     * Makes a batcher.
     *
     * @param max       the most inputs a statement takes; 1 runs each input
     *                  alone, in its caller's thread
     * @param key       each input's key
     * @param statement the statement
     * @throws IllegalArgumentException if the limit is under 1
     */
    public RegistryBatcher(int max, Function<? super I, ?> key,
            Statement<I, O> statement)
    {
        if (max < 1)
        {
            throw new IllegalArgumentException(
                    "A statement takes at least one input");
        }
        this.max = max;
        this.key = key;
        this.statement = statement;
    }

    /**
     * Runs an input, with others waiting at the same time.
     *
     * @param input the input
     * @return its output
     * @throws RuntimeException      the statement's own, as it threw it
     * @throws IllegalStateException if it threw a checked exception, which
     *                               is its cause
     */
    public O run(I input)
    {
        if (max == 1)
        {
            Request<I, O> alone = new Request<>(input, null);
            execute(List.of(alone));
            return outcome(alone);
        }
        Request<I, O> mine = new Request<>(input, lock.newCondition());
        lock.lock();
        try
        {
            queue.add(mine);
            if (!running)
            {
                running = true;
                mine.turn = true;
            }
            while (!mine.done && !mine.turn)
            {
                mine.ready.awaitUninterruptibly();
            }
            if (mine.done)
            {
                return outcome(mine);
            }
        }
        finally
        {
            lock.unlock();
        }
        lead(mine);
        return outcome(mine);
    }

    /**
     * Runs statements of what waits until the caller's own request is done,
     * then gives the turn to the next waiting, if any.
     *
     * @param mine the caller's request
     */
    private void lead(Request<I, O> mine)
    {
        try
        {
            boolean done = false;
            while (!done)
            {
                List<Request<I, O>> batch;
                lock.lock();
                try
                {
                    batch = take();
                }
                finally
                {
                    lock.unlock();
                }
                execute(batch);
                lock.lock();
                try
                {
                    for (Request<I, O> request : batch)
                    {
                        request.done = true;
                        if (request != mine)
                        {
                            request.ready.signal();
                        }
                    }
                    done = mine.done;
                }
                finally
                {
                    lock.unlock();
                }
            }
        }
        finally
        {
            lock.lock();
            try
            {
                Request<I, O> next = queue.peek();
                if (next == null)
                {
                    running = false;
                }
                else
                {
                    next.turn = true;
                    next.ready.signal();
                }
            }
            finally
            {
                lock.unlock();
            }
        }
    }

    /**
     * Takes the next statement's requests from the queue, in order: up to
     * the limit, one of each key. Called holding the lock.
     *
     * @return them; never empty while the caller's own request waits
     */
    private List<Request<I, O>> take()
    {
        List<Request<I, O>> batch = new ArrayList<>();
        Set<Object> keys = new HashSet<>();
        Iterator<Request<I, O>> waiting = queue.iterator();
        while (waiting.hasNext() && batch.size() < max)
        {
            Request<I, O> request = waiting.next();
            if (keys.add(key.apply(request.input)))
            {
                batch.add(request);
                waiting.remove();
            }
        }
        return batch;
    }

    /**
     * Runs one statement for some requests, and gives each its output, or
     * all of them its failure.
     *
     * @param batch the requests
     */
    private void execute(List<Request<I, O>> batch)
    {
        List<I> inputs = new ArrayList<>(batch.size());
        batch.forEach(request -> inputs.add(request.input));
        try
        {
            List<O> outputs = statement.run(inputs);
            if (outputs.size() != batch.size())
            {
                throw new IllegalStateException("A statement gave "
                        + outputs.size() + " outputs for " + batch.size()
                        + " inputs");
            }
            for (int i = 0; i < batch.size(); i++)
            {
                batch.get(i).output = outputs.get(i);
            }
        }
        catch (Exception | Error failure)
        {
            batch.forEach(request -> request.failure = failure);
        }
    }

    /**
     * A request's output, or its failure thrown.
     *
     * @param request the request, done
     * @return its output
     */
    private static <I, O> O outcome(Request<I, O> request)
    {
        Throwable failure = request.failure;
        if (failure instanceof RuntimeException unchecked)
        {
            throw unchecked;
        }
        if (failure instanceof Error error)
        {
            throw error;
        }
        if (failure != null)
        {
            throw new IllegalStateException("A gathered statement failed",
                    failure);
        }
        return request.output;
    }
}
