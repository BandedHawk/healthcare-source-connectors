/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.scenario;

import com.sdh.healthcare.connector.harness.Scenario;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Every scenario, in the order they run: those that read first, then
 * those that change the pipeline, erasure last, since nothing runs on an
 * erased pipeline.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Scenarios
{

    /** Not instantiated. */
    private Scenarios()
    {
    }

    /**
     * Every scenario, in order: erasure last, as nothing runs after it.
     *
     * @return them
     */
    public static List<Scenario> all()
    {
        return List.of(new SupportScenario(), new AccessScenario(),
                new LoadScenario(), new FailuresScenario(),
                new BreakGlassScenario(), new PauseScenario(),
                new KeysScenario(), new OutagesScenario(),
                new FailoverScenario(), new RestoreScenario(),
                new RetentionScenario());
    }

    /**
     * The scenarios named, in the order {@link #all()} runs them.
     *
     * @param names their names, comma-separated; blank for all
     * @return them
     * @throws IllegalArgumentException if a name is no scenario's
     */
    public static List<Scenario> named(String names)
    {
        if (names == null || names.isBlank())
        {
            return all();
        }
        List<String> wanted = new ArrayList<>();
        for (String name : names.split(","))
        {
            wanted.add(name.strip().toLowerCase(Locale.ROOT));
        }
        List<Scenario> chosen = new ArrayList<>();
        for (Scenario scenario : all())
        {
            if (wanted.remove(scenario.name()))
            {
                chosen.add(scenario);
            }
        }
        if (!wanted.isEmpty())
        {
            throw new IllegalArgumentException("No scenario " + wanted
                    + "; the scenarios are " + all().stream()
                            .map(Scenario::name).toList());
        }
        return chosen;
    }
}
