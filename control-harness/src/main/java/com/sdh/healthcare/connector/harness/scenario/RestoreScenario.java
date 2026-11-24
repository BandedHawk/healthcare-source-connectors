/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import static com.sdh.healthcare.connector.harness.Harness.DATA;
import static com.sdh.healthcare.connector.harness.Harness.DATA_2;

import com.sdh.healthcare.connector.harness.Harness;
import com.sdh.healthcare.connector.harness.HarnessFailure;
import com.sdh.healthcare.connector.harness.Reply;
import com.sdh.healthcare.connector.harness.Samples;
import com.sdh.healthcare.connector.harness.Scenario;
import com.sdh.healthcare.connector.harness.Target;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * A restore of the pipeline's database to an earlier point
 * (DESIGN.md §18, HIPAA-OPERATIONS §7): the database backed up while an
 * item is on its way, more items landed and one purged after it, then
 * every worker stopped and the database restored. Each stage's check
 * finds the landing node's newer metadata and the purge the database
 * lacks; its repair, with four eyes, makes it whole; then the workers
 * start, every item is complete once at the split and convert stages,
 * and the purged item is purged again. The release ends the hold on the
 * orphans. The calls made before the restore aren't checked against the
 * audit journal, which went back with the database. Runs only on a target
 * whose store keeps object versions.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RestoreScenario implements Scenario
{

    /** How long a repair, or the pipeline's catching up, may take. */
    static final Duration RUN = Duration.ofMinutes(10);

    /** Creates the scenario. */
    public RestoreScenario()
    {
    }

    @Override
    public String name()
    {
        return "restore";
    }

    @Override
    public String summary()
    {
        return "the database restored to an earlier point, repaired, every"
                + " item complete once, the purged one purged";
    }

    @Override
    public void run(Harness harness) throws Exception
    {
        Target target = harness.target();
        if (!target.restores())
        {
            Harness.say("Skipped: " + target.name() + " doesn't restore"
                    + " here (HIPAA-OPERATIONS §7 is its procedure)");
            return;
        }
        Samples samples = harness.samples();
        String before = Steps.push(harness, Samples.single(samples.document(
                Samples.SYNTHEA, Samples.tag("restore-before"))));
        Steps.complete(harness, Target.CONVERT, before);
        String during = Steps.push(harness, Samples.single(samples.document(
                Samples.ALICE, Samples.tag("restore-during"))));
        String backup = target.backUpDatabase();
        String after = Steps.push(harness, Samples.single(samples.document(
                Samples.MASTER, Samples.tag("restore-after"))));
        List<String> items = List.of(before, during, after);
        for (String itemId : items)
        {
            Steps.complete(harness, Target.CONVERT, itemId);
        }
        // Purged after the point: the database forgets it, the store's
        // control log doesn't.
        String purge = Steps.fourEyes(harness, Target.CONVERT, DATA, DATA_2,
                "/items/" + before + "/purge", Steps.body(
                        "harness purge before a restore", "ticket",
                        "PRIV-7"));
        HarnessFailure.check("done".equals(harness.stage(Target.CONVERT)
                .follow(DATA, purge, RUN).field("state")),
                "The purge didn't finish");

        for (String stage : List.of(Target.LANDING, Target.SPLIT,
                Target.CONVERT))
        {
            target.stopWorkers(stage);
        }
        target.restoreDatabase(backup);
        // The journal is in the database: what was audited since the backup
        // is in the exported events only.
        harness.forgetCalls();

        try
        {
            checkAndRepair(harness);
        }
        catch (Exception failure)
        {
            // So the scenarios after this one have a pipeline to run on.
            for (String stage : List.of(Target.LANDING, Target.SPLIT,
                    Target.CONVERT))
            {
                target.restartWorkers(stage);
            }
            throw failure;
        }
        for (String stage : List.of(Target.LANDING, Target.SPLIT,
                Target.CONVERT))
        {
            HarnessFailure.check(target.restartWorkers(stage),
                    stage + ": its workers didn't start after the repair");
        }

        for (String itemId : List.of(during, after))
        {
            for (String stage : List.of(Target.SPLIT, Target.CONVERT))
            {
                Map<String, Object> progress = settled(harness, stage,
                        itemId);
                HarnessFailure.check(Steps.number(progress, "done") == 1
                        && Steps.number(progress, "expected") == 1
                        && Steps.number(progress, "failed") == 0,
                        stage + ": item " + itemId + " not complete once"
                                + " after the restore: " + progress);
            }
        }
        Harness.await("the purge made again done", RUN, () ->
        {
            Reply purges = harness.stage(Target.CONVERT).get(DATA,
                    "/purges").expect(200);
            HarnessFailure.check(purges.body().contains(before)
                    && purges.body().contains("\"done\""),
                    "not purged again: " + purges.body().length()
                            + " bytes of purges");
            return true;
        });

        for (String stage : harness.stages())
        {
            String id = Steps.fourEyes(harness, stage, DATA, DATA_2,
                    "/restore/release", Steps.body("harness: caught up",
                            "ticket", "INC-7"));
            HarnessFailure.check("done".equals(harness.stage(stage).follow(
                    DATA, id, RUN).field("state")),
                    stage + ": the release didn't finish");
        }
    }

    /**
     * Checks each stage after the restore, then repairs each with four
     * eyes, until it is whole.
     *
     * @param harness the harness
     * @throws Exception if a check or repair doesn't hold
     */
    static void checkAndRepair(Harness harness) throws Exception
    {
        Reply landing = harness.stage(Target.LANDING).get(DATA,
                "/restore/check").expect(200);
        HarnessFailure.check(landing.body().contains("\"forward\""),
                "Landing: no newer metadata found: " + landing);
        Reply convert = harness.stage(Target.CONVERT).get(DATA,
                "/restore/check").expect(200);
        HarnessFailure.check(convert.body().contains("\"to-replay\""),
                "Convert: the purge since isn't found: " + convert);
        Reply refused = harness.stage(Target.CONVERT).action(DATA, "POST",
                "/restore/repair", Steps.body("no ticket"));
        HarnessFailure.check(refused.status() == 400,
                "A repair without a ticket: " + refused);

        for (String stage : harness.stages())
        {
            String id = Steps.fourEyes(harness, stage, DATA, DATA_2,
                    "/restore/repair", Steps.body("harness restore",
                            "ticket", "INC-7"));
            Reply repaired = harness.stage(stage).follow(DATA, id, RUN);
            HarnessFailure.check("done".equals(repaired.field("state"))
                    && repaired.body().contains("\"whole\":true"),
                    stage + ": not whole after its repair (names and counts"
                            + " only): " + repaired.body());
        }
    }

    /**
     * Waits until a stage has an item complete, for longer than an item
     * usually takes: after a restore, the senders' reconcilers send again
     * what the restored registries lack.
     *
     * @param harness the harness
     * @param stage   the stage
     * @param itemId  the item
     * @return its progress once complete
     * @throws Exception if it isn't complete in time
     */
    static Map<String, Object> settled(Harness harness, String stage,
            String itemId) throws Exception
    {
        Object[] last = new Object[1];
        Harness.await("item " + itemId + " complete at " + stage
                + " after the restore", RUN, () ->
                {
                    Map<String, Object> progress = Steps.progress(harness,
                            stage, itemId);
                    last[0] = progress;
                    HarnessFailure.check(progress != null
                            && progress.get("completedAt") != null,
                            "not complete: " + Steps.statusOf(harness,
                                    stage, itemId));
                    return true;
                });
        @SuppressWarnings("unchecked")
        Map<String, Object> progress = (Map<String, Object>) last[0];
        return progress;
    }
}
