/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.OPERATOR;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target;
import java.time.Duration;
import java.util.Map;

/**
 * Pause, drain and resume (CONTROL-API.md): a paused intake refuses
 * pushes with {@code 503}; a paused convert stage takes no new item while
 * the split stage carries on, and completes it once resumed; a drain of
 * the split stage ends once nothing is claimed, leaving it paused until
 * resumed. Nothing is lost.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class PauseScenario implements Scenario
{

    /** How long a paused stage is watched to take nothing. */
    static final Duration HELD = Duration.ofSeconds(30);

    /** Creates the scenario. */
    public PauseScenario()
    {
    }

    @Override
    public String name()
    {
        return "pause";
    }

    @Override
    public String summary()
    {
        return "a paused intake and stage take nothing, a drain settles,"
                + " resume completes";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        Samples samples = harness.samples();
        Steps.pause(harness, Target.LANDING, true);
        byte[] item = Samples.single(samples.document(Samples.ALICE,
                Samples.tag("pause")));
        Harness.await("the paused intake refusing pushes",
                Duration.ofSeconds(30), () -> harness.target().push(item,
                        Map.of()).status() == 503);
        Steps.pause(harness, Target.LANDING, false);
        String landed = Steps.pushOpen(harness, item);

        Steps.pause(harness, Target.CONVERT, true);
        Thread.sleep(10_000);
        String held = Steps.push(harness, Samples.single(samples.document(
                Samples.SYNTHEA, Samples.tag("pause"))));
        Steps.complete(harness, Target.SPLIT, held);
        long until = System.nanoTime() + HELD.toNanos();
        while (System.nanoTime() < until)
        {
            Map<String, Object> progress = Steps.progress(harness,
                    Target.CONVERT, held);
            HarnessFailure.check(progress == null
                    || progress.get("completedAt") == null,
                    "The paused convert stage completed " + held);
            Thread.sleep(2000);
        }
        Steps.pause(harness, Target.CONVERT, false);
        Steps.complete(harness, Target.CONVERT, held);
        Steps.complete(harness, Target.CONVERT, landed);

        Reply drain = harness.stage(Target.SPLIT).action(OPERATOR, "POST",
                "/drain", Steps.body("harness drain")).expect(202);
        Reply drained = harness.stage(Target.SPLIT).follow(OPERATOR,
                drain.field("id"), Duration.ofMinutes(3));
        HarnessFailure.check("done".equals(drained.field("state")),
                "The drain didn't finish: " + drained);
        String during = Steps.push(harness, Samples.single(samples.document(
                Samples.EPIC, Samples.tag("pause"))));
        Thread.sleep(HELD.toMillis());
        Map<String, Object> progress = Steps.progress(harness, Target.SPLIT,
                during);
        HarnessFailure.check(progress == null
                || progress.get("completedAt") == null,
                "The drained split stage took " + during);
        Steps.pause(harness, Target.SPLIT, false);
        Steps.complete(harness, Target.CONVERT, during);
    }
}
