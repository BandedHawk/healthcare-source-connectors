/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.telemetry;

import java.time.Instant;
import java.util.SortedMap;

/**
 * Where audit events are kept durably (HIPAA.md §3.1, audit controls), as
 * well as exported: {@link Telemetry} appends every audit event to the one
 * installed with {@link Telemetry#journal(AuditJournal)}. An event holds
 * only allow-listed attributes, so a journal holds no PHI.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface AuditJournal
{

    /** A journal that keeps nothing: for a process configured without one. */
    AuditJournal NONE = event ->
    {
    };

    /**
     * An audit event, as a journal receives it.
     *
     * @param at         when it was emitted
     * @param name       its name, such as {@code sdh.audit.item.landed}
     * @param severity   {@code INFO}, {@code WARN} or {@code ERROR}
     * @param attributes its allow-listed attributes, by key, as text
     */
    record Event(Instant at, String name, String severity,
            SortedMap<String, String> attributes)
    {
    }

    /**
     * Appends an event. May wait, while the journal can't keep up or can't
     * write, rather than lose it; never throws.
     *
     * @param event the event
     */
    void append(Event event);

    /**
     * Writes what has been appended, waiting a short while at most: before
     * something that may stop the process. Does nothing by default.
     */
    default void flush()
    {
    }
}
