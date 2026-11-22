/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.harness.scenario.Scenarios;
import com.sdh.healthcare.connector.harness.testcontainers.TestcontainersTarget;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The harness at its first level (CONTROL-API-DESIGN.md §14): every
 * scenario, or those {@code sdh.harness.scenarios} names, against the
 * pipeline on Testcontainers. Run by {@code scripts/harness.sh
 * testcontainers}, which builds the stages' jars first.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class HarnessIT
{

    /** Creates the test; JUnit runs each test on a new instance. */
    HarnessIT()
    {
    }

    /**
     * Every scenario passes.
     *
     * @throws Exception if the pipeline can't be started
     */
    @Test
    void everyScenarioPasses() throws Exception
    {
        Path root = Path.of(System.getProperty("sdh.harness.root", ".."))
                .toAbsolutePath().normalize();
        List<Scenario> scenarios = Scenarios.named(System.getProperty(
                "sdh.harness.scenarios", ""));
        List<Harness.Result> results = HarnessRun.run(
                new TestcontainersTarget(root), root, scenarios);
        assertThat(results).hasSameSizeAs(scenarios)
                .allSatisfy(result -> assertThat(result.passed())
                        .as(result.scenario() + ": " + result.outcome())
                        .isTrue());
    }
}
