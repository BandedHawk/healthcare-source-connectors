/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.io.IOException;

/**
 * Something a request carries that is read only when the operation needs
 * it: a reason, a corrected record, a public key. The CLI reads it from
 * standard input at that point, so its usage errors come in the order they
 * always have; the control API gives it from the request's body.
 *
 * @param <T> what is given
 * @author Jonas Barnett
 * @since 0.1.0
 */
@FunctionalInterface
public interface Given<T>
{

    /**
     * Reads what is given.
     *
     * @return it
     * @throws IOException if it can't be read
     */
    T get() throws IOException;

    /**
     * Something already in hand.
     *
     * @param value it
     * @param <T>   its type
     * @return it, given
     */
    static <T> Given<T> of(T value)
    {
        return () -> value;
    }
}
