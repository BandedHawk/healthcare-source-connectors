/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The security configuration guide ({@code docs/security/CONFIGURATION.md},
 * HIPAA.md §11.2 D6) names every check of the {@code hipaa} profile, so a
 * check added to {@link ComplianceCheck} can't be left out of what
 * customers are told.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ConfigurationGuideTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    ConfigurationGuideTest()
    {
    }

    /**
     * Every check's ID is in the guide's table, as a row of its own.
     *
     * @throws IOException if the guide can't be read
     */
    @Test
    void namesEveryCheck() throws IOException
    {
        String guide = Files.readString(ReferenceConfigurationTest.ROOT
                .resolve("docs/security/CONFIGURATION.md"));
        List<String> missing = new ArrayList<>();
        for (ComplianceCheck check : ComplianceCheck.values())
        {
            if (!guide.contains("\n| `" + check.id() + "` |"))
            {
                missing.add(check.id());
            }
        }
        assertThat(missing).as("checks the guide's §1 leaves out").isEmpty();
    }
}
