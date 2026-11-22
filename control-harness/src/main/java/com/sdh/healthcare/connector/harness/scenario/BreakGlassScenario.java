/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.AUDITOR;
import static com.sdh.healthcare.connector.harness.Harness.BREAK_GLASS;
import static com.sdh.healthcare.connector.harness.Harness.BREAK_GLASS_2;
import static com.sdh.healthcare.connector.harness.Harness.VIEWER;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Break-glass (CONTROL-API.md): a plaintext restore of a landed item needs
 * {@code phi.break-glass} and a ticket, waits for a second person, is run
 * once approved, and its result is fetched once by its requester only;
 * the ticket is audited.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class BreakGlassScenario implements Scenario
{

    /** Creates the scenario. */
    public BreakGlassScenario()
    {
    }

    @Override
    public String name()
    {
        return "breakglass";
    }

    @Override
    public String summary()
    {
        return "a plaintext restore: its permission, a ticket, a second"
                + " person, fetched once";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        Instant start = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        String tag = Samples.tag("glass");
        String itemId = Steps.push(harness, Samples.single(harness.samples()
                .document(Samples.SYNTHEA, tag)));
        Steps.complete(harness, Target.SPLIT, itemId);
        String path = "/items/" + itemId + "/restore/plaintext";
        String ticket = "INC-" + start.getEpochSecond();
        Reply refused = harness.stage(Target.SPLIT).action(VIEWER, "POST",
                path, Steps.body("no break-glass", "ticket", ticket));
        HarnessFailure.check(refused.status() == 403,
                "A restore without phi.break-glass: " + refused);
        Reply unticketed = harness.stage(Target.SPLIT).action(BREAK_GLASS,
                "POST", path, Steps.body("no ticket"));
        HarnessFailure.check(unticketed.status() == 400,
                "A restore without a ticket: " + unticketed);
        String id = Steps.fourEyes(harness, Target.SPLIT, BREAK_GLASS,
                BREAK_GLASS_2, path, Steps.body("harness break-glass",
                        "ticket", ticket));
        Reply done = harness.stage(Target.SPLIT).follow(BREAK_GLASS, id,
                Steps.ITEM);
        HarnessFailure.check("done".equals(done.field("state")),
                "The restore didn't finish: " + done);
        Reply other = harness.stage(Target.SPLIT).get(BREAK_GLASS_2,
                "/operations/" + id + "/result");
        HarnessFailure.check(other.status() == 403,
                "Another fetched the result: " + other);
        Reply result = harness.stage(Target.SPLIT).get(BREAK_GLASS,
                "/operations/" + id + "/result").expect(200);
        HarnessFailure.check(result.body().contains(tag),
                "The result isn't the item restored");
        Reply again = harness.stage(Target.SPLIT).get(BREAK_GLASS,
                "/operations/" + id + "/result");
        HarnessFailure.check(again.status() == 410,
                "The result fetched twice: " + again);
        Reply events = harness.stage(Target.SPLIT).get(AUDITOR,
                "/audit/events?from=" + start.minusSeconds(5)).expect(200);
        HarnessFailure.check(events.body().contains(ticket),
                "The ticket wasn't audited");
    }
}
