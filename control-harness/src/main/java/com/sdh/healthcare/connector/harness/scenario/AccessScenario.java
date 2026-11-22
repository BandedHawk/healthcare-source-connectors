/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.AUDITOR;
import static com.sdh.healthcare.connector.harness.Harness.NOBODY;
import static com.sdh.healthcare.connector.harness.Harness.VIEWER;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Scenario;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Access (CONTROL-API.md): on every stage, a call without a credential is
 * refused as unauthenticated and audited; a caller with no role, or
 * without the operation's permission, is refused and audited as denied;
 * one with it is answered. The harness then finds every call in the
 * stage's audit journal with its principal, and the journal unchanged.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AccessScenario implements Scenario
{

    /** Creates the scenario. */
    public AccessScenario()
    {
    }

    @Override
    public String name()
    {
        return "access";
    }

    @Override
    public String summary()
    {
        return "each operation refused without a credential or its"
                + " permission, and audited";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        Instant start = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        for (String stage : harness.stages())
        {
            Reply none = harness.stage(stage).get(null, "/health");
            HarnessFailure.check(none.status() == 401,
                    stage + ": no credential, yet " + none);
            Reply nobody = harness.stage(stage).get(NOBODY, "/health");
            HarnessFailure.check(nobody.status() == 403
                    && "permission-required".equals(nobody.problem()),
                    stage + ": no role, yet " + nobody);
            harness.stage(stage).get(VIEWER, "/health").expect(200);
            Map<String, Map<String, Object>> actions = Map.of(
                    "/pause", Steps.body("not mine to do"),
                    "/keys/rotate", Steps.body("not mine to do", "kek", "x"),
                    "/erase", Steps.body("not mine to do", "ticket", "T-1"));
            for (Map.Entry<String, Map<String, Object>> call
                    : actions.entrySet())
            {
                Reply refused = harness.stage(stage).action(VIEWER, "POST",
                        call.getKey(), call.getValue());
                HarnessFailure.check(refused.status() == 403,
                        stage + ": " + call.getKey() + " without its"
                                + " permission, yet " + refused);
            }
            for (String path : new String[] {"/audit/verify",
                "/audit/events", "/retention", "/keys"})
            {
                Reply refused = harness.stage(stage).get(VIEWER, path);
                HarnessFailure.check(refused.status() == 403,
                        stage + ": " + path + " without its permission, yet "
                                + refused);
            }
            Reply events = harness.stage(stage).get(AUDITOR,
                    "/audit/events?from=" + start.minusSeconds(5))
                    .expect(200);
            HarnessFailure.check(events.body().contains(
                    "\"sdh.audit.control.unauthenticated\""), stage
                            + ": the call without a credential wasn't"
                            + " audited");
        }
    }
}
