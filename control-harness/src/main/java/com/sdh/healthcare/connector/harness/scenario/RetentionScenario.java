/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.DATA;
import static com.sdh.healthcare.connector.harness.Harness.DATA_2;
import static com.sdh.healthcare.connector.harness.Harness.OPERATOR;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target;
import java.time.Duration;
import java.util.List;

/**
 * Retention and erasure (CONTROL-API.md): retention set within its
 * bounds and refused outside them; an item purged at every stage it
 * passed, each with a ticket and a second person; then the pipeline
 * paused, drained and erased stage by stage, each answering a signed
 * certificate, leaving nothing in the store, and its workers refusing to
 * start. Runs last: nothing runs on an erased pipeline.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RetentionScenario implements Scenario
{

    /** How long a purge or an erasure may take. */
    static final Duration RUN = Duration.ofMinutes(6);

    /** Creates the scenario. */
    public RetentionScenario()
    {
    }

    @Override
    public String name()
    {
        return "erasure";
    }

    @Override
    public String summary()
    {
        return "retention within bounds, an item purged, the pipeline"
                + " erased";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        String convert = Target.CONVERT;
        Reply retention = harness.stage(convert).get(DATA, "/retention")
                .expect(200);
        long minimum = Steps.number(retention.json(), "minimumDays");
        harness.stage(convert).action(DATA, "PUT", "/retention", Steps.body(
                "harness retention", "days", minimum + 1)).expect(200);
        Reply below = harness.stage(convert).action(DATA, "PUT",
                "/retention", Steps.body("below the floor", "days",
                        Math.max(0, minimum - 1)));
        HarnessFailure.check(below.status() == 400,
                "Retention below its floor: " + below);
        harness.stage(convert).action(DATA, "PUT", "/retention", Steps.body(
                "harness retention back", "forever", true)).expect(200);

        String itemId = Steps.push(harness, Samples.single(harness.samples()
                .document(Samples.MASTER, Samples.tag("purge"))));
        Steps.complete(harness, convert, itemId);
        for (String stage : List.of(Target.CONVERT, Target.SPLIT,
                Target.LANDING))
        {
            String id = Steps.fourEyes(harness, stage, DATA, DATA_2,
                    "/items/" + itemId + "/purge", Steps.body(
                            "harness purge", "ticket", "PRIV-1"));
            Reply purged = harness.stage(stage).follow(DATA, id, RUN);
            HarnessFailure.check("done".equals(purged.field("state")),
                    stage + ": the purge didn't finish: " + purged);
            HarnessFailure.check(harness.stage(stage).get(DATA,
                    "/purges").expect(200).body().contains(itemId),
                    stage + ": the purge isn't listed");
        }

        for (String stage : harness.stages())
        {
            Steps.pause(harness, stage, true);
        }
        for (String stage : List.of(Target.SPLIT, Target.CONVERT))
        {
            Reply drain = harness.stage(stage).action(OPERATOR, "POST",
                    "/drain", Steps.body("harness: before erasure"))
                    .expect(202);
            HarnessFailure.check("done".equals(harness.stage(stage).follow(
                    OPERATOR, drain.field("id"), RUN).field("state")),
                    stage + ": not drained");
        }
        for (String stage : harness.stages())
        {
            String id = Steps.fourEyes(harness, stage, DATA, DATA_2,
                    "/erase", Steps.body("harness: end of contract",
                            "ticket", "END-1"));
            Reply erased = harness.stage(stage).follow(DATA, id, RUN);
            HarnessFailure.check("done".equals(erased.field("state")),
                    stage + ": the erasure didn't finish: " + erased);
            HarnessFailure.check(erased.body().contains("\"certificate\""),
                    stage + ": no erasure certificate");
            List<String> left = harness.target().stored(stage);
            HarnessFailure.check(left.isEmpty(), stage + ": " + left.size()
                    + " objects left in the store");
            HarnessFailure.check(!harness.target().restartWorkers(stage),
                    stage + ": its workers started after erasure");
        }
    }
}
