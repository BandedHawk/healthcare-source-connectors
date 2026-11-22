/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.VIEWER;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Scenario;
import java.util.List;
import java.util.Map;

/**
 * Support without PHI (CONTROL-API-DESIGN.md §15.9): each stage's health
 * reaches every dependency; its settings, compliance outcome and support
 * bundle answer, and none of them holds any secret the target configured.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SupportScenario implements Scenario
{

    /** Creates the scenario. */
    public SupportScenario()
    {
    }

    @Override
    public String name()
    {
        return "support";
    }

    @Override
    public String summary()
    {
        return "health, settings, compliance and the support bundle, with"
                + " no secret";
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run(Harness harness) throws Exception
    {
        for (String stage : harness.stages())
        {
            Reply health = harness.stage(stage).get(VIEWER, "/health")
                    .expect(200);
            for (Object each : (List<Object>) health.json().get("checks"))
            {
                Map<String, Object> check = (Map<String, Object>) each;
                HarnessFailure.check(Boolean.TRUE.equals(check.get(
                        "reached")), stage + ": " + check.get("name") + " "
                        + check.get("target") + " not reached ("
                        + check.get("problem") + ")");
            }
            HarnessFailure.check(Boolean.TRUE.equals(health.json().get("up")),
                    stage + " not up: " + health);
            Reply settings = harness.stage(stage).get(VIEWER, "/settings")
                    .expect(200);
            HarnessFailure.check(((Number) settings.json().get("redacted"))
                    .intValue() > 0, stage + ": no setting redacted");
            Reply compliance = harness.stage(stage).get(VIEWER,
                    "/compliance").expect(200);
            HarnessFailure.check(compliance.field("profile") != null,
                    stage + ": no profile in " + compliance);
            Reply bundle = harness.stage(stage).get(VIEWER,
                    "/support-bundle").expect(200);
            for (String part : List.of("build", "health", "settings",
                    "compliance", "control", "status", "operations"))
            {
                HarnessFailure.check(bundle.json().containsKey(part),
                        stage + ": the bundle has no " + part);
            }
            for (Reply answer : List.of(health, settings, compliance,
                    bundle))
            {
                for (String secret : harness.target().secrets())
                {
                    HarnessFailure.check(!answer.body().contains(secret),
                            stage + ": a secret in " + answer.path());
                }
            }
        }
    }
}
