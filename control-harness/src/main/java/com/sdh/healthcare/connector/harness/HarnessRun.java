/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Runs scenarios against a target, each after the one before whatever
 * became of it, and reports each one's outcome. The target is closed
 * afterwards: what it started is stopped.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class HarnessRun
{

    /** Not instantiated. */
    private HarnessRun()
    {
    }

    /**
     * Runs scenarios against a target, then closes it.
     *
     * @param target    the target, started
     * @param root      the repository's root, for the samples
     * @param scenarios the scenarios, in order
     * @return each one's outcome
     * @throws Exception if the target can't be reached at all
     */
    public static List<Harness.Result> run(Target target, Path root,
            List<Scenario> scenarios) throws Exception
    {
        List<Harness.Result> results = new ArrayList<>();
        try (target)
        {
            Harness harness = new Harness(target, new Samples(root));
            for (Scenario scenario : scenarios)
            {
                results.add(harness.run(scenario));
            }
        }
        report(target.name(), results);
        return results;
    }

    /**
     * Prints the outcomes.
     *
     * @param target  the target's name
     * @param results the outcomes
     */
    static void report(String target, List<Harness.Result> results)
    {
        Harness.say("Results on " + target + ":");
        for (Harness.Result result : results)
        {
            Harness.say(String.format(Locale.ROOT, "  %-10s %-6s %5d s  %s",
                    result.scenario(), result.passed() ? "PASS" : "FAIL",
                    result.took().toSeconds(), result.passed() ? ""
                            : result.outcome()));
        }
    }
}
