package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;

/**
 * The audit event for one admin command, filled in as the command learns things (file ID, offset, bytes), so a
 * failure is still reported with whatever was known. Allow-listed {@link Attr} keys only.
 */
final class AuditRecord {
    private Fields fields;

    AuditRecord(String operator) {
        this.fields = Fields.of(Attr.OPERATOR, operator);
    }

    void add(Attr key, Object value) {
        fields = fields.with(key, value);
    }

    Fields fields() {
        return fields;
    }
}
