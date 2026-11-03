/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.config;

/**
 * What every connector application's settings record has, so the shared
 * runtime, such as telemetry export, can read them whichever application
 * it runs in.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface ConnectorSettings
{

    /**
     * The connector's ID, recorded with its checkpoints and messages.
     *
     * @return the ID
     */
    String connectorId();

    /**
     * The single-instance lease.
     *
     * @return the lease settings
     */
    CommonSettings.Lease lease();

    /**
     * The keys and who holds them.
     *
     * @return the key settings
     */
    CommonSettings.Crypto crypto();

    /**
     * The queue messages are published to.
     *
     * @return the queue settings
     */
    CommonSettings.Queue queue();

    /**
     * How transient failures are retried.
     *
     * @return the retry settings
     */
    CommonSettings.Retry retry();

    /**
     * The telemetry export.
     *
     * @return the telemetry settings
     */
    CommonSettings.Telemetry telemetry();
}
