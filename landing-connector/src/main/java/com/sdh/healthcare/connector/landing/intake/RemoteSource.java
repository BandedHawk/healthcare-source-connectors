/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import java.io.IOException;
import java.util.List;

/**
 * A source on a server, listed by the node itself rather than by a Camel
 * consumer (design §5.3.4). It keeps one connection open and makes a new
 * one when that breaks. Only the intake's route thread uses it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface RemoteSource extends IntakeSource, AutoCloseable
{

    /**
     * Lists the items on the source: hidden names, markers, renamed items
     * and the {@code move-to} directory left out, the include pattern
     * applied, oldest first.
     *
     * @return the items
     * @throws IntakeUnavailableException if the server can't be reached,
     *                                    logged into, or listed
     * @throws IOException                if the listing fails otherwise
     */
    List<ListedItem> list() throws IOException;

    /**
     * Tells whether the last call to the server worked.
     *
     * @return true if it did, or if there has been none
     */
    @Override
    boolean reachable();

    /** Closes the connection; the next call makes a new one. */
    @Override
    void close();
}
