/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

/**
 * One part of a result read on its own, so that a part that can't be read
 * (an unreachable KMS, a database down) leaves the others shown, as the
 * CLI's status has always done ({@link StatusParts}).
 *
 * @param value   what was read; null if it couldn't be
 * @param failure why it couldn't be; null if it was. Its message is shown
 *                to the CLI's operator only, never by the control API
 * @param <T>     what the part holds
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Partial<T>(T value, Exception failure)
{

    /**
     * Reads a part.
     *
     * @param <T> what it holds
     */
    @FunctionalInterface
    public interface Reading<T>
    {

        /**
         * Reads it.
         *
         * @return what was read
         * @throws Exception if it can't be read
         */
        T read() throws Exception;
    }

    /**
     * Reads a part, keeping its failure rather than throwing it.
     *
     * @param reading how to read it
     * @param <T>     what it holds
     * @return the part
     */
    public static <T> Partial<T> of(Reading<T> reading)
    {
        try
        {
            return new Partial<>(reading.read(), null);
        }
        catch (Exception failure)
        {
            return new Partial<>(null, failure);
        }
    }

    /**
     * What was read, or the failure again.
     *
     * @return the value
     * @throws Exception why it couldn't be read
     */
    public T get() throws Exception
    {
        if (failure != null)
        {
            throw failure;
        }
        return value;
    }
}
