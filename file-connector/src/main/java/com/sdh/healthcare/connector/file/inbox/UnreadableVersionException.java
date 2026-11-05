/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.inbox;

import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;

/**
 * An {@code item.landed} message of a contract major version this splitter
 * can't read (design §10.2). It isn't acknowledged: it is delivered again,
 * with a growing pause, until the splitter is upgraded. The splitter must
 * be upgraded before the landing node starts writing a new major version
 * (docs/DEPLOYMENT.md).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class UnreadableVersionException extends Exception
{

    /**
     * Creates the exception.
     *
     * @param cause why the message was refused
     */
    public UnreadableVersionException(MessageRejectedException cause)
    {
        super("item.landed message of a contract version this splitter"
                + " can't read; upgrade the splitter", cause);
    }
}
