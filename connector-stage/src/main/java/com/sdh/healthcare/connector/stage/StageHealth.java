/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.sdh.healthcare.connector.broker.receive.BrokerHealth;
import com.sdh.healthcare.connector.broker.receive.BrokerProblem;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Whether a stage's input broker answers, as its watch or route last
 * found. A change is logged once, without PHI.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class StageHealth implements BrokerHealth
{

    /** Logs changes. */
    private static final Logger LOG =
            LoggerFactory.getLogger(StageHealth.class);

    /** What is wrong; null while the broker answers. */
    private volatile BrokerProblem problem;

    /** Creates a health that starts out answering. */
    public StageHealth()
    {
    }

    @Override
    public void answered()
    {
        if (problem != null)
        {
            LOG.info("stage.input_reachable");
            problem = null;
        }
    }

    @Override
    public void unreachable(BrokerProblem reason)
    {
        if (problem != reason)
        {
            LOG.warn("stage.input_unreachable reason={}", reason);
            problem = reason;
        }
    }

    /**
     * What is wrong with the broker, as last found.
     *
     * @return the problem; empty while it answers
     */
    public Optional<BrokerProblem> problem()
    {
        return Optional.ofNullable(problem);
    }
}
