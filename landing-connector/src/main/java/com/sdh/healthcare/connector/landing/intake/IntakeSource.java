/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.runtime.InboundDirectoryClaim;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

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
     * The item's replaces marker, {@code <name>.sdh-replaces}, if the
     * producer left one beside it (design §5.3.8); {@link #open} reads it.
     *
     * @param item the item
     * @return the marker; empty if there is none, or the source can't
     *         have one
     * @throws IOException if the source can't be read
     */
    default Optional<ListedItem> replacesMarker(ListedItem item)
            throws IOException
    {
        return Optional.empty();
    }

    /**
     * The earlier item an item replaces, as its source gives it, such as a
     * table snapshot's {@code sdh.replaces} property (design §5.3.8). It
     * is checked as a marker's is; a source that gives one has no
     * markers.
     *
     * @param item the item
     * @return the item ID as given; empty if none
     */
    default Optional<String> replacesValue(ListedItem item)
    {
        return Optional.empty();
    }

    /**
     * Where a read-write remote source keeps its claim (design §9.5),
     * through its own client.
     *
     * @return the claim file; empty if the source keeps none here
     */
    default Optional<InboundDirectoryClaim.ClaimFile> claimFile()
    {
        return Optional.empty();
    }

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
