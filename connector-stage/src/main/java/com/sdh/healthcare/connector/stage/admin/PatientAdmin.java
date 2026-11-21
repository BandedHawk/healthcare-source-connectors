/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * What {@link PatientsCommand} reads from: a record stage's output, such
 * as gold at the transform stage, record by record.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface PatientAdmin
{

    /**
     * A stored record, without its content.
     *
     * @param itemId      its item
     * @param seq         its number in the item
     * @param recordId    its ID
     * @param contentType what its content is, such as
     *                    {@code application/fhir+json}
     */
    record StoredRecord(UUID itemId, long seq, String recordId,
            String contentType)
    {
    }

    /**
     * Given each record found, with a way to read its content.
     */
    @FunctionalInterface
    interface Visitor
    {

        /**
         * Takes a record.
         *
         * @param record  the record
         * @param content reads its content, decrypted; each read is
         *                audited
         * @throws Exception if it can't be taken
         */
        void visit(StoredRecord record, ContentReader content)
                throws Exception;
    }

    /**
     * Reads a record's content.
     */
    @FunctionalInterface
    interface ContentReader
    {

        /**
         * Reads it.
         *
         * @return the decrypted content; the caller clears it
         * @throws Exception if it can't be read
         */
        byte[] read() throws Exception;
    }

    /**
     * The stage, as its registry names it.
     *
     * @return the stage, such as {@code transform}
     */
    String stage();

    /**
     * Where the command reads the operator's public key, and identifiers
     * to look for: standard input.
     *
     * @return the stream
     */
    default InputStream patientInput()
    {
        return System.in;
    }

    /**
     * Visits the stored records of items, every copy and revision.
     *
     * @param itemIds the items
     * @param visitor given each record
     * @throws Exception if the output can't be read
     */
    void records(List<UUID> itemIds, Visitor visitor) throws Exception;

    /**
     * Visits the records stored in a span of time, every copy and
     * revision.
     *
     * @param from    the start of the span
     * @param to      its end, exclusive
     * @param visitor given each record
     * @throws Exception if the output can't be read
     */
    void records(Instant from, Instant to, Visitor visitor) throws Exception;
}
