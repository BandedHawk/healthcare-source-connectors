/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.record.runtime;

import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.record.RecordTransform;
import com.sdh.healthcare.connector.record.config.RecordStageSettings;
import com.sdh.healthcare.connector.runtime.FatalExit;
import com.sdh.healthcare.connector.runtime.telemetry.TelemetryConfiguration;
import org.apache.camel.CamelContext;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

/**
 * A record stage's Spring wiring (STAGES.md §17.3): the runtime, and
 * starting its duties and input once the application is up. A stage's
 * application gives only its {@link RecordTransform} and its settings,
 * a {@link RecordStageSettings}; with no transform, nothing here is set
 * up.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@AutoConfiguration
@ConditionalOnBean(RecordTransform.class)
@Import(TelemetryConfiguration.class)
public class RecordStageConfiguration
{

    /** Created by Spring. */
    public RecordStageConfiguration()
    {
    }

    /**
     * Exits the process on a fatal error; tests put their own in its place.
     *
     * @return the exit hook
     */
    @Bean
    @ConditionalOnMissingBean
    FatalExit fatalExit()
    {
        return FatalExit.exitProcess();
    }

    /**
     * Sets up the runtime. Takes {@link Telemetry} so the SDK is installed
     * before anything is recorded.
     *
     * @param settings  the stage's settings
     * @param transform the stage's own step
     * @param camel     the Camel context
     * @param fatal     the exit hook
     * @param telemetry the installed telemetry
     * @return the runtime, its input and duties not yet started
     * @throws Exception if it can't be set up
     */
    @Bean(destroyMethod = "close")
    RecordStageRuntime recordStageRuntime(RecordStageSettings settings,
            RecordTransform transform, CamelContext camel, FatalExit fatal,
            Telemetry telemetry) throws Exception
    {
        return RecordStageRuntime.create(settings, transform, camel, fatal);
    }

    /**
     * Starts the duties and the input once the application is up, unless
     * {@code sdh.stage.autostart} is false: for tests that hand messages
     * to the worker themselves.
     *
     * @param settings the stage's settings
     * @param runtime  the runtime
     * @return the startup step
     */
    @Bean
    ApplicationRunner startRecordStage(RecordStageSettings settings,
            RecordStageRuntime runtime)
    {
        return arguments ->
        {
            if (settings.stage().autostart())
            {
                runtime.start();
            }
        };
    }
}
