/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.audit;

import java.time.Instant;
import java.util.UUID;

/**
 * Where a reader's decryptions of stored records are recorded (HIPAA.md
 * §4.3, accounting of disclosures; §3.1, audit controls): who read which
 * record, from which table, and why. A record names no content: its IDs,
 * its table, and the reader's own name and purpose code.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@FunctionalInterface
public interface ReadAudit
{

    /**
     * One record read.
     *
     * @param at        when it was opened
     * @param reader    who read it: the name the reader was built with,
     *                  such as a service account
     * @param purpose   why, as a code such as {@code TREAT},
     *                  {@code HPAYMT} or {@code PUBHLTH}; null if none was
     *                  given
     * @param itemId    the item the record is of
     * @param seq       its number in the item
     * @param recordId  its ID, keyed over its content
     * @param namespace the table's namespace
     * @param table     the table
     */
    record RecordRead(Instant at, String reader, String purpose, UUID itemId,
            long seq, String recordId, String namespace, String table)
    {
    }

    /**
     * Records a read. Must not throw; may wait rather than lose it.
     *
     * @param read what was read
     */
    void recorded(RecordRead read);
}
