/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Failover (CONTROL-API.md, "Nothing is lost"): each stage's workers are
 * killed with items on their way, with no shutdown, as a lost node would
 * leave them, and come back. Every item then completes, each of its
 * records split, sent and converted as many times as it has records:
 * nothing is lost, and nothing is done twice.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class FailoverScenario implements Scenario
{

    /** Creates the scenario. */
    public FailoverScenario()
    {
    }

    @Override
    public String name()
    {
        return "failover";
    }

    @Override
    public String summary()
    {
        return "each stage's workers killed part-way; every item completes,"
                + " once";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        Map<String, Integer> items = new LinkedHashMap<>();
        for (String stage : List.of(Target.SPLIT, Target.CONVERT))
        {
            for (int index = 0; index < 3; index++)
            {
                items.put(Steps.push(harness, batch(harness)), 3);
            }
            Harness.say("Killing the " + stage + " stage's workers");
            harness.target().kill(stage);
        }
        Harness.say("Killing the landing node");
        harness.target().kill(Target.LANDING);
        items.put(Steps.pushOpen(harness, batch(harness)), 3);
        for (Map.Entry<String, Integer> item : items.entrySet())
        {
            Map<String, Object> converted = Steps.complete(harness,
                    Target.CONVERT, item.getKey());
            Map<String, Object> split = Steps.progress(harness, Target.SPLIT,
                    item.getKey());
            long records = item.getValue();
            HarnessFailure.check(Steps.number(converted, "expected") == records
                    && Steps.number(converted, "done") == records
                    && Steps.number(converted, "failed") == 0,
                    "Item " + item.getKey() + " not converted once: "
                            + converted);
            HarnessFailure.check(split != null
                    && Steps.number(split, "expected") == records
                    && Steps.number(split, "done") == records,
                    "Item " + item.getKey() + " not split once: " + split);
        }
    }

    /**
     * An item of three documents.
     *
     * @param harness the harness
     * @return its bytes
     * @throws Exception if the samples can't be read
     */
    static byte[] batch(Harness harness) throws Exception
    {
        Samples samples = harness.samples();
        return Samples.batch(samples.document(Samples.SYNTHEA,
                Samples.tag("failover")), samples.document(Samples.ALICE,
                        Samples.tag("failover")), samples.document(
                                Samples.EPIC, Samples.tag("failover")));
    }
}
