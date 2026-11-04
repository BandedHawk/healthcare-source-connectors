/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;

/**
 * The audit event for one admin command, filled in as the command learns
 * things (file ID, offset, bytes), so a failure is still reported with
 * whatever was known. Allow-listed {@link Attr} keys only.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class AuditRecord
{

    /** The fields known so far. */
    private Fields fields;

    /**
     * Starts an event for an operator.
     *
     * @param operator who ran the command
     */
    AuditRecord(String operator)
    {
        this.fields = Fields.of(Attr.OPERATOR, operator);
    }

    /**
     * Adds a field.
     *
     * @param key the allow-listed key
     * @param value the value
     */
    void add(Attr key, Object value)
    {
        fields = fields.with(key, value);
    }

    /**
     * The fields known so far.
     *
     * @return the fields
     */
    Fields fields()
    {
        return fields;
    }
}
