/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.harness.aws.AwsTarget;
import com.sdh.healthcare.connector.harness.sandbox.SandboxTarget;
import com.sdh.healthcare.connector.harness.scenario.Scenarios;
import com.sdh.healthcare.connector.harness.testcontainers.TestcontainersTarget;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The harness (CONTROL-API-DESIGN.md §14): every scenario, or those
 * {@code sdh.harness.scenarios} names, against the target {@code
 * sdh.harness.target} names: {@code testcontainers} (the default), the
 * pipeline on Testcontainers, {@code sandbox}, the sandbox's harness
 * pipeline, or {@code aws}, the AWS PoC's. Run by {@code
 * scripts/harness.sh}, which builds or deploys the target first.
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
        List<Harness.Result> results = HarnessRun.run(target(root), root,
                scenarios);
        assertThat(results).hasSameSizeAs(scenarios)
                .allSatisfy(result -> assertThat(result.passed())
                        .as(result.scenario() + ": " + result.outcome())
                        .isTrue());
    }

    /**
     * The target {@code sdh.harness.target} names.
     *
     * @param root the repository's root
     * @return it, started or reached
     * @throws Exception if it can't be
     */
    static Target target(Path root) throws Exception
    {
        String name = System.getProperty("sdh.harness.target",
                "testcontainers");
        return switch (name)
        {
            case "testcontainers" -> new TestcontainersTarget(root);
            case "sandbox" -> new SandboxTarget(Path.of(System.getProperty(
                    "sdh.harness.state")), System.getProperty(
                            "sdh.harness.profile", "sdh-demo"));
            case "aws" -> new AwsTarget(Path.of(System.getProperty(
                    "sdh.harness.state")), System.getProperty(
                            "sdh.harness.profile", "claude-poc"));
            default -> throw new IllegalArgumentException("No target "
                    + name + "; they are testcontainers, sandbox and aws");
        };
    }
}
