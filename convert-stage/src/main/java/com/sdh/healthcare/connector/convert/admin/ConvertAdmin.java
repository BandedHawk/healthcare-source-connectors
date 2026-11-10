/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.admin;

import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.convert.config.ConvertProperties;
import com.sdh.healthcare.connector.convert.runtime.ConvertConfiguration;
import com.sdh.healthcare.connector.record.admin.RecordStageAdmin;
import com.sdh.healthcare.connector.stage.admin.AdminCli;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * The convert stage's operator commands (design §11.4): the shared record
 * stage's, {@code status}, {@code reprocess}, {@code drain-check},
 * {@code version} and those for the fail store, run as
 * {@code convert-stage.jar admin <command>}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConvertAdmin
{

    /** Not instantiated. */
    private ConvertAdmin()
    {
    }

    /**
     * The convert stage's CLI.
     *
     * @return the CLI
     */
    public static AdminCli<ConvertProperties, ?> cli()
    {
        return RecordStageAdmin.cli("convert-stage.jar", "convert-stage",
                Configuration.class, ConvertProperties.class,
                StageMessages.CONVERT, ConvertConfiguration::transform);
    }

    /**
     * Binds {@code sdh.*} and nothing else: not a component, so the
     * stage's own scan never picks it up.
     */
    @EnableConfigurationProperties(ConvertProperties.class)
    static class Configuration
    {

        /** Created by Spring. */
        Configuration()
        {
        }
    }
}
