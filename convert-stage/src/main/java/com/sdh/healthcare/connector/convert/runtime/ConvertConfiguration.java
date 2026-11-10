/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.runtime;

import com.sdh.healthcare.connector.convert.ConvertTransform;
import com.sdh.healthcare.connector.convert.config.ConvertProperties;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.stage.StageBuild;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The convert stage's own beans: its settings and its
 * {@link ConvertTransform}. The shared record stage's auto-configuration
 * runs the stage with them (STAGES.md §17.3), started unless
 * {@code sdh.stage.autostart} is false.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration
@EnableConfigurationProperties(ConvertProperties.class)
public class ConvertConfiguration
{

    /** Created by Spring. */
    public ConvertConfiguration()
    {
    }

    /**
     * The stage's step, from its settings.
     *
     * @param properties the stage's settings
     * @return the transform
     */
    @Bean
    ConvertTransform convertTransform(ConvertProperties properties)
    {
        return transform(properties);
    }

    /**
     * The stage's step, from its settings: for the stage and its operator
     * commands alike.
     *
     * @param properties the stage's settings
     * @return the transform, with every converter on the class path
     * @throws IllegalArgumentException if the largest record is not
     *                                  positive
     */
    public static ConvertTransform transform(ConvertProperties properties)
    {
        ConvertProperties.Convert convert = properties.convert();
        return new ConvertTransform(ConverterRegistry.load(),
                new ConvertTransform.Settings(convert.options(),
                        convert.maxBytes().toBytes(),
                        StageBuild.identity(ConvertTransform.class)));
    }
}
