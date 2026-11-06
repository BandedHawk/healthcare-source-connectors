/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link StageBuild}: the build that starts a processor's version.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class StageBuildTest
{

    /** Creates the tests. */
    StageBuildTest()
    {
    }

    /** A build is named by its version and the time it was built. */
    @Test
    void namesTheBuildByVersionAndTime()
    {
        Properties info = new Properties();
        info.setProperty("build.version", "0.1.0-SNAPSHOT");
        info.setProperty("build.time", "2026-11-06T10:15:30.120Z");
        assertThat(StageBuild.identity(info))
                .isEqualTo("0.1.0-SNAPSHOT@2026-11-06T10:15:30.120Z");
    }

    /**
     * A stage built without its details (this module has none) is
     * {@code unknown}, and doesn't fail to start; so are missing details.
     */
    @Test
    void aBuildWithoutDetailsIsUnknown()
    {
        assertThat(StageBuild.identity(StageBuildTest.class))
                .isEqualTo(StageBuild.UNKNOWN);
        assertThat(StageBuild.identity(new Properties()))
                .isEqualTo("unknown@unknown");
    }
}
