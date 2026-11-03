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
 * @param version its version: for a file, its modification time in
 *                milliseconds
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ListedItem(String ref, long size, long version)
{

    /**
     * Creates an item, checking it has a reference.
     *
     * @param ref     the item's path within the source
     * @param size    its size in bytes
     * @param version its version
     * @throws NullPointerException if the reference is null
     */
    public ListedItem
    {
        Objects.requireNonNull(ref, "ref");
    }

    /**
     * The version as the ledger records it.
     *
     * @return the version, in decimal
     */
    public String versionText()
    {
        return Long.toString(version);
    }
}
