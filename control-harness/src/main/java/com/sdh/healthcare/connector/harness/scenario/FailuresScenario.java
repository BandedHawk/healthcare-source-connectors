/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.EXPORTER;
import static com.sdh.healthcare.connector.harness.Harness.OPERATOR;
import static com.sdh.healthcare.connector.harness.Harness.VIEWER;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target;
import com.sdh.healthcare.connector.harness.Unsealer;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Failures and remediation (CONTROL-API.md): a document that doesn't
 * convert fails at the convert stage and the rest of its batch completes;
 * the failure is listed without its content, exported sealed to the
 * exporter's own key and opened, then corrected. An item cut off part-way
 * fails at the split stage; it is declined, and its corrected version is
 * pushed as a new item linked to it: the failure is kept, never replaced.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class FailuresScenario implements Scenario
{

    /** Creates the scenario. */
    public FailuresScenario()
    {
    }

    @Override
    public String name()
    {
        return "failures";
    }

    @Override
    public String summary()
    {
        return "failures listed, exported sealed, corrected, declined;"
                + " a resubmission linked";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        Samples samples = harness.samples();
        String good = samples.document(Samples.SYNTHEA, Samples.tag("fail"));
        String broken = Samples.unconvertible(samples.document(Samples.ALICE,
                Samples.tag("fail")));
        String batch = Steps.push(harness, Samples.batch(good, broken));
        Map<String, Object> progress = Steps.settled(harness,
                Target.CONVERT, batch);
        HarnessFailure.check(Steps.number(progress, "failed") == 1
                && Steps.number(progress, "expected") == 2,
                "Not one record of two failed: " + progress);
        HarnessFailure.check(progress.get("completedAt") == null,
                "An item complete with a failure open: " + progress);
        Map<String, Object> failure = open(harness, Target.CONVERT, batch);
        long seq = Steps.number(failure, "seq");
        HarnessFailure.check(!failure.toString().contains("&nbsp;"),
                "A failure listed with its content");

        harness.sealingKey(Target.CONVERT, EXPORTER);
        String path = "/failures/" + batch + "/" + seq;
        Reply refused = harness.stage(Target.CONVERT).post(OPERATOR,
                path + "/export", Steps.body("export without phi.export",
                        "sealTo", EXPORTER.fingerprint()));
        HarnessFailure.check(refused.status() == 403,
                "An export without phi.export: " + refused);
        Reply sealed = harness.stage(Target.CONVERT).post(EXPORTER,
                path + "/export", Steps.body("harness export",
                        "sealTo", EXPORTER.fingerprint())).expect(200);
        String opened = new String(Unsealer.unseal(sealed.body(),
                EXPORTER.sealing().getPrivate()), UTF_8);
        HarnessFailure.check(opened.contains("&nbsp;(amended)"),
                "The export isn't the record that failed");
        harness.note("convertFailure", path);

        String corrected = opened.replace("&nbsp;", " ");
        harness.stage(Target.CONVERT).action(EXPORTER, "POST",
                path + "/correct", Steps.body("harness correction",
                        "record", Base64.getEncoder().encodeToString(
                                corrected.getBytes(UTF_8)))).expect(200);
        Harness.await("the corrected record done", Steps.ITEM, () ->
                !isOpen(state(harness, Target.CONVERT, batch, seq)));
        Steps.complete(harness, Target.CONVERT, batch);

        String first = samples.document(Samples.EPIC, Samples.tag("cut"));
        String second = samples.document(Samples.MASTER, Samples.tag("cut"));
        String cut = Steps.push(harness, Samples.truncated(first, second));
        Harness.await("the cut-off item failed at the split stage",
                Steps.ITEM, () -> !harness.stage(Target.SPLIT).get(VIEWER,
                        "/failures?itemId=" + cut).expect(200).body()
                        .contains("\"failures\":[]"));
        Map<String, Object> splitFailure = open(harness, Target.SPLIT, cut);
        long splitSeq = Steps.number(splitFailure, "seq");
        harness.stage(Target.SPLIT).action(OPERATOR, "POST", "/failures/"
                + cut + "/" + splitSeq + "/decline",
                Steps.body("harness decline: resent corrected")).expect(200);
        HarnessFailure.check("DECLINED".equals(state(harness, Target.SPLIT,
                cut, splitSeq)), "Not declined");
        String resent = Steps.push(harness, Samples.batch(first, second),
                Map.of("Replaces", cut));
        Steps.complete(harness, Target.CONVERT, resent);
        HarnessFailure.check("DECLINED".equals(state(harness, Target.SPLIT,
                cut, splitSeq)), "The declined failure changed");
    }

    /**
     * An item's one open failure at a stage.
     *
     * @param harness the harness
     * @param stage   the stage
     * @param itemId  the item
     * @return the failure, as listed
     * @throws Exception if it has none open
     */
    @SuppressWarnings("unchecked")
    static Map<String, Object> open(Harness harness, String stage,
            String itemId) throws Exception
    {
        Reply listed = harness.stage(stage).get(VIEWER, "/failures?itemId="
                + itemId).expect(200);
        for (Object each : (List<Object>) listed.json().get("failures"))
        {
            Map<String, Object> failure = (Map<String, Object>) each;
            if (isOpen(String.valueOf(failure.get("state"))))
            {
                return failure;
            }
        }
        throw new HarnessFailure(stage + ": no open failure for " + itemId);
    }

    /**
     * A failure's state.
     *
     * @param harness the harness
     * @param stage   the stage
     * @param itemId  the item
     * @param seq     the failure's record
     * @return its state
     * @throws Exception if it isn't listed
     */
    @SuppressWarnings("unchecked")
    static String state(Harness harness, String stage, String itemId,
            long seq) throws Exception
    {
        Reply listed = harness.stage(stage).get(VIEWER, "/failures?itemId="
                + itemId + "&all=true").expect(200);
        for (Object each : (List<Object>) listed.json().get("failures"))
        {
            Map<String, Object> failure = (Map<String, Object>) each;
            if (Steps.number(failure, "seq") == seq)
            {
                return String.valueOf(failure.get("state"));
            }
        }
        throw new HarnessFailure(stage + ": failure " + itemId + "/" + seq
                + " not listed");
    }

    /**
     * Whether a failure is still open.
     *
     * @param state its state
     * @return true if failed or awaiting a revision
     */
    static boolean isOpen(String state)
    {
        return "FAILED".equals(state) || "REVISION_PENDING".equals(state);
    }
}
