/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.broker.receive.ReceiveProperties;
import com.sdh.healthcare.connector.broker.receive.ReceiveSettings;
import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import java.time.Duration;
import java.util.Set;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * A stage's settings, {@code sdh.stage} (STAGES.md §7). The input's batch
 * is also the commit batch: each batch received is committed in one
 * append. Secrets come from the environment, never the file.
 *
 * @param input          the broker the stage receives from
 * @param output         where its messages go
 * @param dlq            where conflicts go: the stage's dead-letter queue
 * @param concurrency    how many records a pod processes at once
 * @param claim          how records are claimed
 * @param registrySchema the Postgres schema of the stage's registry, in
 *                       the lease database
 * @param sendTimeout    the longest to wait for the broker to take a
 *                       message
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record StageProperties(ReceiveProperties input,
        CommonSettings.Queue output, CommonSettings.Queue dlq,
        @DefaultValue("4") int concurrency, @DefaultValue Claim claim,
        @DefaultValue("sdh_stage") String registrySchema,
        @DefaultValue("30s") Duration sendTimeout)
{

    /** Where the settings are configured, to name them in errors. */
    public static final String PREFIX = "sdh.stage";

    /**
     * How records are claimed (STAGES.md §6).
     *
     * @param ttl           how long a claim lasts unless renewed
     * @param renewInterval how often a pod renews its claims
     * @param busyPoll      how long to wait before looking again at a
     *                      record another pod holds
     */
    public record Claim(@DefaultValue("60s") Duration ttl,
            @DefaultValue("15s") Duration renewInterval,
            @DefaultValue("1s") Duration busyPoll)
    {
    }

    /**
     * The checked input settings.
     *
     * @param attributes the message attributes or headers to read
     * @return the settings
     * @throws IllegalArgumentException if a setting is missing or would let
     *                                  a message be acknowledged before it
     *                                  is stored
     */
    public ReceiveSettings receive(Set<String> attributes)
    {
        if (input == null)
        {
            throw new IllegalArgumentException(PREFIX + ".input is required");
        }
        return ReceiveSettings.of(input, PREFIX + ".input", attributes);
    }

    /**
     * The checked worker settings.
     *
     * @return the settings
     * @throws IllegalArgumentException if the output or dead-letter queue
     *                                  is missing, or a value is out of
     *                                  range
     */
    public StageWorker.Settings worker()
    {
        requireQueue(output, "output");
        requireQueue(dlq, "dlq");
        try
        {
            return new StageWorker.Settings(concurrency, claim.ttl(),
                    claim.renewInterval(), claim.busyPoll(), sendTimeout);
        }
        catch (IllegalArgumentException invalid)
        {
            throw new IllegalArgumentException(PREFIX + ": "
                    + invalid.getMessage(), invalid);
        }
    }

    /**
     * Checks that a queue is configured.
     *
     * @param queue the queue
     * @param name  its setting's name
     * @throws IllegalArgumentException if its type or URI is missing
     */
    private static void requireQueue(CommonSettings.Queue queue, String name)
    {
        if (queue == null || queue.type() == null || queue.type().isBlank()
                || queue.uri() == null || queue.uri().isBlank())
        {
            throw new IllegalArgumentException(PREFIX + "." + name
                    + ".type and " + PREFIX + "." + name + ".uri are required");
        }
    }
}
