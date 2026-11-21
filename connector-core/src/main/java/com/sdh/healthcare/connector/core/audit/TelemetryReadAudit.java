/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.audit;

import com.sdh.healthcare.connector.contract.audit.ReadAudit;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;

/**
 * Records a reader's reads as {@code sdh.audit.reader.record_read} audit
 * events (HIPAA.md §4.3): exported with the stages' events and kept in the
 * process's durable audit journal when one is installed
 * ({@link JdbcAuditJournal}). Give it to connector-queue-reader's
 * {@code QueueReader.Builder.audit}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TelemetryReadAudit implements ReadAudit
{

    /** Creates the audit. */
    public TelemetryReadAudit()
    {
    }

    @Override
    public void recorded(RecordRead read)
    {
        Telemetry.get().audit("reader.record_read",
                Fields.of(Attr.READER, read.reader())
                        .with(Attr.PURPOSE, read.purpose())
                        .with(Attr.ITEM_ID, read.itemId())
                        .with(Attr.SEQ, read.seq())
                        .with(Attr.RECORD_ID, read.recordId())
                        .with(Attr.TABLE, read.namespace() + "."
                                + read.table()));
    }
}
