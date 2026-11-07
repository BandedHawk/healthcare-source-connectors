/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

/**
 * The convert stage's settings, as Spring binds them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ConvertPropertiesTest
{

    /** Creates the tests; JUnit runs each on a new instance. */
    ConvertPropertiesTest()
    {
    }

    /**
     * No format options is none: the stage started without any used to
     * fail, since the options digested into each record's version were
     * null.
     */
    @Test
    void noOptionsIsNone()
    {
        Binder binder = new Binder(new MapConfigurationPropertySource(
                Map.of("sdh.convert.max-bytes", "1MB")));

        ConvertProperties.Convert convert = binder
                .bind("sdh.convert", ConvertProperties.Convert.class).get();

        assertThat(convert.options()).isEmpty();
        assertThat(convert.maxBytes().toBytes()).isEqualTo(1024 * 1024);
    }
}
