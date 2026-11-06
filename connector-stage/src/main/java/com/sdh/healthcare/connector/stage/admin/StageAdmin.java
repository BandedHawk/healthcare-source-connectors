/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.stage.RegistryReport;

/**
 * What {@link StageStatusCommand} reads from, for a stage on the shared
 * worker and registry (STAGES.md §7).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface StageAdmin
{

    /**
     * The stage, as its registry and duties lease name it.
     *
     * @return the stage, such as {@code convert}
     */
    String stage();

    /**
     * The stage's connector ID, which names its duties lease.
     *
     * @return the ID
     */
    String connectorId();

    /**
     * The leases, to inspect.
     *
     * @return the lease store
     */
    JdbcLeaseStore leases();

    /**
     * The stage's registry, to read.
     *
     * @return the report
     */
    RegistryReport registry();
}
