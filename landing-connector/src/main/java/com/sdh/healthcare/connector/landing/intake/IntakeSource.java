/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import java.io.IOException;
import java.io.InputStream;

/**
 * Where an intake's items come from (design §5.3.4): reads them, says
 * whether it can be reached, and deals with an item once it is landed.
 * Listing is done by the intake's Camel route, which hands each item it
 * finds to {@link IntakeService}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface IntakeSource
{

    /**
     * What the source's items are, as {@code landed_items} and
     * {@code item.landed} record it.
     *
     * @return {@link ItemKind#FILE}, unless the source says otherwise
     */
    default ItemKind kind()
    {
        return ItemKind.FILE;
    }

    /**
     * Opens an item's bytes from a position.
     *
     * @param item   the item
     * @param offset where to start, in bytes
     * @return the bytes from there to the end
     * @throws ItemLander.ItemChangedException if the item isn't the
     *                                         version listed
     * @throws IOException                     if the item can't be read
     */
    InputStream open(ListedItem item, long offset) throws IOException;

    /**
     * Deletes, moves or renames a landed item, as configured; does nothing
     * on a read-only source. An item that has gone, or changed since it
     * was listed, is left alone: a changed item is a new version, to be
     * landed in its turn.
     *
     * @param item the landed item
     * @throws IOException if the source can't be changed
     */
    void afterLanding(ListedItem item) throws IOException;

    /**
     * Tells whether the source can be listed now.
     *
     * @return true if it can
     */
    boolean reachable();

    /**
     * Checks, at startup, that a read-write source can be changed, so an
     * item is never landed that couldn't then be removed. Does nothing on
     * a read-only source.
     *
     * @throws IntakeNotWritableException if it can't be changed
     * @throws IOException                 if a remote source can't be
     *                                     reached to check
     */
    void checkWritable() throws IOException;
}
