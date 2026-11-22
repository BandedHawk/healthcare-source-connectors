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
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Load and complete (CONTROL-API.md): synthetic documents end to end, one
 * at a time and in batches; each item complete at the split and convert
 * stages exactly once, every record done and none failed; and nothing
 * left idle once they are.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class LoadScenario implements Scenario
{

    /** Creates the scenario. */
    public LoadScenario()
    {
    }

    @Override
    public String name()
    {
        return "load";
    }

    @Override
    public String summary()
    {
        return "synthetic documents end to end, every item complete once";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        Samples samples = harness.samples();
        Map<String, Integer> expected = Map.of(
                Steps.push(harness, Samples.single(samples.document(
                        Samples.SYNTHEA, Samples.tag("load")))), 1,
                Steps.push(harness, Samples.single(samples.document(
                        Samples.ALICE, Samples.tag("load")))), 1,
                Steps.push(harness, Samples.batch(
                        samples.document(Samples.MASTER, Samples.tag("load")),
                        samples.document(Samples.EPIC, Samples.tag("load")))),
                2);
        List<String> items = new ArrayList<>(expected.keySet());
        for (String itemId : items)
        {
            for (String stage : List.of(Target.SPLIT, Target.CONVERT))
            {
                Map<String, Object> progress = Steps.complete(harness, stage,
                        itemId);
                HarnessFailure.check(Steps.number(progress, "expected")
                        == expected.get(itemId)
                        && Steps.number(progress, "done")
                                == expected.get(itemId)
                        && Steps.number(progress, "failed") == 0,
                        stage + ": item " + itemId + " not complete once: "
                                + progress);
            }
        }
        String stuck = harness.stage(Target.CONVERT).get(VIEWER,
                "/stuck?idle=1").expect(200).body();
        for (String itemId : items)
        {
            HarnessFailure.check(!stuck.contains(itemId),
                    "Item " + itemId + " stuck");
        }
        harness.note("loaded", items);
    }
}
