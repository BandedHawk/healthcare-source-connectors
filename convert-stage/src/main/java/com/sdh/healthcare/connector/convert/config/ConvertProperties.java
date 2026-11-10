/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.config;

import com.sdh.healthcare.connector.record.config.RecordStageSettings;
import com.sdh.healthcare.connector.record.config.RecordStageSettings.Maintenance;
import com.sdh.healthcare.connector.record.config.RecordStageSettings.Reconcile;
import com.sdh.healthcare.connector.record.config.RecordStageSettings.Results;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Crypto;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Lease;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Retry;
import com.sdh.healthcare.connector.runtime.config.CommonSettings.Telemetry;
import com.sdh.healthcare.connector.stage.StageProperties;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;

/**
 * Every {@code sdh.*} setting of the convert stage (STAGES.md §9, §17.7):
 * those of every record stage, and {@code sdh.convert}. The settings
 * every application shares are in
 * {@link com.sdh.healthcare.connector.runtime.config.CommonSettings}.
 * Secrets normally come from environment variables or a Spring
 * {@code configtree:} mount.
 *
 * @param connectorId the stage's ID, naming its duties lease
 * @param convert     how records are converted
 * @param lease       the lease database and timings, for the duties lease;
 *                    the stage's registry is in the same database
 * @param crypto      the keys: the records KEK, signing, recipient and
 *                    inbox keys; no landing KEK
 * @param stage       the split stage's output it receives from, where its
 *                    messages go, its dead-letter queue, its registry, and
 *                    its source ({@code split_records}) and sink
 *                    ({@code records})
 * @param results     the producer's results
 * @param reconcile   how records whose messages were lost are found
 * @param retry       how transient failures are retried
 * @param maintenance table maintenance of the sink
 * @param telemetry   the telemetry export
 * @author Jonas Barnett
 * @since 0.1.0
 */
@ConfigurationProperties("sdh")
public record ConvertProperties(
        @DefaultValue("convert-stage") String connectorId,
        @DefaultValue Convert convert,
        @DefaultValue Lease lease,
        @DefaultValue Crypto crypto,
        @DefaultValue StageProperties stage,
        @DefaultValue Results results,
        @DefaultValue Reconcile reconcile,
        @DefaultValue Retry retry,
        @DefaultValue Maintenance maintenance,
        @DefaultValue Telemetry telemetry)
        implements RecordStageSettings
{

    /**
     * {@code sdh.convert.memory}, if set.
     *
     * @return the bytes; 0 for a share of the heap
     */
    @Override
    public long memoryBytes()
    {
        return convert.memory() == null ? 0 : convert.memory().toBytes();
    }

    /**
     * How records are converted (design §6.4).
     *
     * @param options  format options as in design §6.2, applied to every
     *                 format, such as {@code namespace}
     * @param maxBytes records larger than this are stored as split,
     *                 {@code CONVERT_TOO_LARGE}
     * @param memory   the heap the conversions running at once may use;
     *                 unset for a share of the heap
     */
    public record Convert(Map<String, String> options,
            @DefaultValue("64MB") DataSize maxBytes, DataSize memory)
    {

        /**
         * No options set is none, not null.
         */
        public Convert
        {
            options = options == null ? Map.of() : Map.copyOf(options);
        }
    }
}
