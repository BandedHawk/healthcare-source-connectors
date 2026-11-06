/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.admin;

import com.sdh.healthcare.connector.convert.ConvertProcessor;
import com.sdh.healthcare.connector.convert.config.ConvertProperties;
import com.sdh.healthcare.connector.core.checkpoint.JdbcLeaseStore;
import com.sdh.healthcare.connector.stage.RegistryReport;
import com.sdh.healthcare.connector.stage.admin.AdminDatabase;
import com.sdh.healthcare.connector.stage.admin.StageAdmin;

/**
 * What the convert stage's admin commands read from: the lease database
 * only, opened on first use, read-only (design §11.4).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ConvertAdminContext implements StageAdmin, AutoCloseable
{

    /** The stage's settings. */
    private final ConvertProperties properties;

    /** The lease database. */
    private final AdminDatabase database;

    /**
     * Creates the context; nothing is opened yet.
     *
     * @param properties the stage's settings
     */
    ConvertAdminContext(ConvertProperties properties)
    {
        this.properties = properties;
        this.database = new AdminDatabase(properties.lease());
    }

    @Override
    public String stage()
    {
        return ConvertProcessor.STAGE;
    }

    @Override
    public String connectorId()
    {
        return properties.connectorId();
    }

    @Override
    public JdbcLeaseStore leases()
    {
        return database.leases();
    }

    @Override
    public RegistryReport registry()
    {
        return new RegistryReport(database.dataSource(),
                properties.stage().registrySchema(), ConvertProcessor.STAGE);
    }

    @Override
    public void close()
    {
        database.close();
    }
}
