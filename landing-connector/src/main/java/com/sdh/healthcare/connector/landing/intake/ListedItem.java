/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

import java.util.Objects;

/**
 * An item as one listing of a source saw it. The reference may hold PHI,
 * such as a patient's name in a filename, so it is never logged; the node
 * keeps only its keyed hash.
 *
 * @param ref     the item's path within the source, with {@code /}
 *                between names
 * @param size    its size in bytes
 * @param version its modification time in milliseconds
 * @param tag     what else tells versions apart: an object's ETag or
 *                generation; null for a file
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ListedItem(String ref, long size, long version, String tag)
{

    /**
     * Creates an item, checking it has a reference.
     *
     * @param ref     the item's path within the source
     * @param size    its size in bytes
     * @param version its modification time
     * @param tag     its ETag or generation, if it has one
     * @throws NullPointerException if the reference is null
     */
    public ListedItem
    {
        Objects.requireNonNull(ref, "ref");
    }

    /**
     * Creates a file's item, which has no tag.
     *
     * @param ref     the item's path within the source
     * @param size    its size in bytes
     * @param version its modification time
     */
    public ListedItem(String ref, long size, long version)
    {
        this(ref, size, version, null);
    }

    /**
     * The version as the ledger records it: the time in decimal, then
     * {@code :} and the tag if there is one. An object written again with
     * the same bytes has a new time, so it is a new version, as a file
     * would be.
     *
     * @return the version
     */
    public String versionText()
    {
        return tag == null ? Long.toString(version) : version + ":" + tag;
    }

    /**
     * Tells whether another sighting is the same version of this item.
     *
     * @param other the other sighting
     * @return true if the size, time and tag are the same
     */
    public boolean sameVersion(ListedItem other)
    {
        return size == other.size && version == other.version
                && Objects.equals(tag, other.tag);
    }
}
