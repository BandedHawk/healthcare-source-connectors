/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.harness.kube;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A pipeline's stages' worker pods in a cluster, as the harness's
 * Kubernetes targets restart and kill them: by deleting them, gracefully
 * as a rollout stops them, or at once, as a lost node leaves them, and
 * waiting for their Deployment's replacement.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Workers
{

    /** How long workers may take to be ready again. */
    public static final Duration READY = Duration.ofMinutes(5);

    /** Reads the cluster's answers. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** The cluster. */
    private final Kubectl kubectl;

    /** The pipeline's namespace. */
    private final String namespace;

    /** Each stage's workers' {@code app} label. */
    private final Map<String, String> apps;

    /**
     * A pipeline's workers.
     *
     * @param kubectl   the cluster
     * @param namespace the pipeline's namespace
     * @param apps      each stage's workers' {@code app} label
     */
    public Workers(Kubectl kubectl, String namespace, Map<String, String> apps)
    {
        this.kubectl = kubectl;
        this.namespace = namespace;
        this.apps = Map.copyOf(apps);
    }

    /**
     * Deletes the stage's worker pods, gracefully, as a rollout stops
     * them, and waits for their replacement.
     *
     * @param stage the stage
     * @return whether a replacement started; false if it refused to
     * @throws Exception if neither happens in time
     */
    public boolean restart(String stage) throws Exception
    {
        Set<String> before = pods(stage);
        kubectl.kubectl("-n", namespace, "delete", "pod", "-l", "app="
                + apps.get(stage), "--wait=true", "--timeout=180s");
        return replaced(stage, before);
    }

    /**
     * Deletes the stage's worker pods at once, with no shutdown, and waits
     * for their replacement.
     *
     * @param stage the stage
     * @throws Exception if no replacement starts
     */
    public void kill(String stage) throws Exception
    {
        Set<String> before = pods(stage);
        kubectl.kubectl("-n", namespace, "delete", "pod", "-l", "app="
                + apps.get(stage), "--grace-period=0", "--force",
                "--wait=false");
        if (!replaced(stage, before))
        {
            throw new IllegalStateException(stage + "'s workers didn't start"
                    + " again after being killed");
        }
    }

    /**
     * A stage's worker pods.
     *
     * @param stage the stage
     * @return their names
     * @throws IOException if they can't be listed
     */
    private Set<String> pods(String stage) throws IOException
    {
        Set<String> names = new HashSet<>();
        for (JsonNode pod : workers(stage))
        {
            names.add(pod.path("metadata").path("name").asText());
        }
        return names;
    }

    /**
     * A stage's worker pods, as the cluster describes them.
     *
     * @param stage the stage
     * @return them
     * @throws IOException if they can't be listed
     */
    private JsonNode workers(String stage) throws IOException
    {
        return JSON.readTree(kubectl.kubectl("-n", namespace, "get", "pods",
                "-l", "app=" + apps.get(stage), "-o", "json")).path("items");
    }

    /**
     * Waits until a new worker pod is ready, or has refused to start.
     *
     * @param stage  the stage
     * @param before the pods that were there
     * @return true once a new pod is ready; false if one exited instead
     * @throws Exception if neither happens in time
     */
    private boolean replaced(String stage, Set<String> before)
            throws Exception
    {
        long until = System.nanoTime() + READY.toNanos();
        while (System.nanoTime() < until)
        {
            for (JsonNode pod : workers(stage))
            {
                if (before.contains(pod.path("metadata").path("name")
                        .asText()) || pod.path("metadata").has(
                                "deletionTimestamp"))
                {
                    continue;
                }
                for (JsonNode container : pod.path("status")
                        .path("containerStatuses"))
                {
                    if (container.path("ready").asBoolean())
                    {
                        return true;
                    }
                    if (container.path("restartCount").asInt() > 0
                            || container.path("state").has("terminated"))
                    {
                        return false;
                    }
                }
            }
            Thread.sleep(3000);
        }
        throw new IllegalStateException(stage + "'s workers weren't ready,"
                + " nor stopped, in " + READY);
    }
}
