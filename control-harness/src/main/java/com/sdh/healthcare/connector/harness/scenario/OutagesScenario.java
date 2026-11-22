/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.OPERATOR;
import static com.sdh.healthcare.connector.harness.Harness.VIEWER;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target.Dependency;
import com.sdh.healthcare.connector.harness.Target;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Outages (CONTROL-API.md): the database, the broker, the store and the
 * key service are each cut off for a while, with an item on its way, and
 * brought back. Every item then completes, any found stuck reprocessed
 * through the API; nothing is lost.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class OutagesScenario implements Scenario
{

    /** How long each service is cut off. */
    static final Duration CUT = Duration.ofSeconds(30);

    /** Creates the scenario. */
    public OutagesScenario()
    {
    }

    @Override
    public String name()
    {
        return "outages";
    }

    @Override
    public String summary()
    {
        return "database, broker, store and KMS cut off and back; nothing"
                + " lost";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        List<String> items = new ArrayList<>();
        for (Dependency dependency : Dependency.values())
        {
            byte[] before = Samples.single(harness.samples().document(
                    Samples.SYNTHEA, Samples.tag("outage")));
            items.add(Steps.push(harness, before));
            Harness.say("Cutting off " + dependency);
            harness.target().cut(dependency);
            try
            {
                Thread.sleep(CUT.toMillis());
            }
            finally
            {
                harness.target().restore(dependency);
                Harness.say("Restored " + dependency);
            }
            items.add(Steps.pushOpen(harness, Samples.single(harness
                    .samples().document(Samples.ALICE,
                            Samples.tag("outage")))));
        }
        List<String> stuck = new ArrayList<>();
        for (String itemId : items)
        {
            try
            {
                Steps.complete(harness, Target.CONVERT, itemId);
            }
            catch (HarnessFailure notYet)
            {
                stuck.add(itemId);
            }
        }
        if (!stuck.isEmpty())
        {
            Harness.say("Reprocessing items not complete: " + stuck);
            for (String stage : List.of(Target.SPLIT, Target.CONVERT))
            {
                Reply run = harness.stage(stage).action(OPERATOR, "POST",
                        "/reprocess-runs", Steps.body("harness: after an"
                                + " outage", "itemIds", stuck));
                HarnessFailure.check(run.status() == 200
                        || run.status() == 202, "Reprocess refused: " + run);
            }
            for (String itemId : stuck)
            {
                Steps.complete(harness, Target.CONVERT, itemId);
            }
        }
        String idle = harness.stage(Target.CONVERT).get(VIEWER,
                "/stuck?idle=2").expect(200).body();
        for (String itemId : items)
        {
            HarnessFailure.check(!idle.contains(itemId),
                    "Item " + itemId + " left stuck");
        }
    }
}
