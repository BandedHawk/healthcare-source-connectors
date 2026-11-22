/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.EXPORTER;
import static com.sdh.healthcare.connector.harness.Harness.KEYS;
import static com.sdh.healthcare.connector.harness.Harness.KEYS_2;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target;
import com.sdh.healthcare.connector.harness.Unsealer;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Keys (CONTROL-API.md): the convert stage's KEK is rotated and new data
 * is stored under it; then what it stored before is re-keyed, with a
 * second person's approval, until nothing is left under the old KEK; and
 * a failure stored before is still exported and opened: still readable.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class KeysScenario implements Scenario
{

    /** How long a re-key may take: a few maintenance runs. */
    static final Duration REKEY = Duration.ofMinutes(6);

    /** How long the pods are given to take a rotation. */
    static final Duration TAKEN = Duration.ofSeconds(15);

    /** Creates the scenario. */
    public KeysScenario()
    {
    }

    @Override
    public String name()
    {
        return "keys";
    }

    @Override
    public String summary()
    {
        return "a KEK rotated, stored data re-keyed, still readable, the old"
                + " KEK unused";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        String stage = Target.CONVERT;
        String old = harness.target().kek(stage);
        String next = harness.target().otherKek(stage);
        Reply before = harness.stage(stage).get(KEYS, "/keys").expect(200);
        HarnessFailure.check(old.equals(before.field("active")),
                "Not wrapping with " + old + ": " + before);
        Reply rotated = harness.stage(stage).action(KEYS, "POST",
                "/keys/rotate", Steps.body("harness rotation", "kek", next))
                .expect(200);
        HarnessFailure.check(next.equals(rotated.field("active")),
                "Not rotated: " + rotated);
        // Each pod takes the rotation at its next look, every few seconds.
        Thread.sleep(TAKEN.toMillis());
        String itemId = Steps.push(harness, Samples.single(harness.samples()
                .document(Samples.SYNTHEA, Samples.tag("keys"))));
        Steps.complete(harness, stage, itemId);

        String id = Steps.fourEyes(harness, stage, KEYS, KEYS_2,
                "/keys/rekey", Steps.body("harness re-key", "from", old,
                        "to", next));
        Reply done = harness.stage(stage).follow(KEYS, id, REKEY);
        HarnessFailure.check("done".equals(done.field("state")),
                "The re-key didn't finish: " + done);
        Map<String, Object> after = harness.stage(stage).get(KEYS, "/keys")
                .expect(200).json();
        for (String part : List.of("tables", "rows"))
        {
            HarnessFailure.check(under(after.get(part), old) == 0,
                    "Still under " + old + " in " + part + ": "
                            + after.get(part));
        }
        HarnessFailure.check(!(after.get("idKeys") instanceof Map<?, ?> ids
                && ids.containsValue(old)), "An ID key still under " + old
                        + ": " + after.get("idKeys"));
        Object failure = harness.note("convertFailure");
        if (failure != null)
        {
            Reply sealed = harness.stage(stage).post(EXPORTER,
                    failure + "/export", Steps.body("export after re-key",
                            "sealTo", EXPORTER.fingerprint())).expect(200);
            HarnessFailure.check(new String(Unsealer.unseal(sealed.body(),
                    EXPORTER.sealing().getPrivate()), UTF_8).contains(
                            "(amended)"),
                    "A failure stored before the re-key isn't readable");
        }
    }

    /**
     * How much an answer counts under a KEK: the counts it names the KEK
     * for, at any depth.
     *
     * @param answer the answer, or part of one
     * @param kek    the KEK
     * @return the sum
     */
    static long under(Object answer, String kek)
    {
        long sum = 0;
        if (answer instanceof Map<?, ?> map)
        {
            for (Map.Entry<?, ?> entry : map.entrySet())
            {
                if (kek.equals(entry.getKey())
                        && entry.getValue() instanceof Number count)
                {
                    sum += count.longValue();
                }
                else
                {
                    sum += under(entry.getValue(), kek);
                }
            }
        }
        else if (answer instanceof List<?> list)
        {
            for (Object each : list)
            {
                sum += under(each, kek);
            }
        }
        return sum;
    }
}
