/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.util.Map;

/**
 * One run of an operation: who runs it, its audit event, and where what it
 * streams goes.
 *
 * @param caller who runs it
 * @param audit  the run's audit event, which the operation adds what it
 *               learns to; its operator is the caller's principal
 * @param sink   where streamed rows and sealed parts go
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Invocation(Caller caller, AuditRecord audit, Sink sink)
{

    /**
     * Where an operation streams what is too large, or too sensitive, to
     * return as one result: rows (the audit journal's events) or sealed
     * parts (JWE, CONTROL-API-DESIGN.md §7).
     */
    public interface Sink
    {

        /**
         * Streams a row. It holds no PHI.
         *
         * @param row the row, by field
         */
        void row(Map<String, Object> row);

        /**
         * Streams a sealed part.
         *
         * @param jwe the part, in compact JWE
         */
        void sealed(String jwe);

        /**
         * Takes a part of a plaintext result, for a break-glass operation
         * a second person approved (CONTROL-API-DESIGN.md §6). Only the
         * control API's runner takes one, and holds it encrypted until
         * its requester fetches it; no caller is sent one as it comes.
         *
         * @param part the part's bytes
         * @throws UnsupportedOperationException if this sink takes none,
         *                                       as none but the runner's
         *                                       does
         */
        default void plaintext(byte[] part)
        {
            throw new UnsupportedOperationException("Only an approved"
                    + " break-glass operation's result holds plaintext");
        }
    }

    /**
     * A run whose operator is the audit event's.
     *
     * @return the caller's name
     */
    public String principal()
    {
        return caller.principal();
    }
}
