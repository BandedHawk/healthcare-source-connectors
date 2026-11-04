/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.intake;

/**
 * Why a remote source can't be listed or read (design §5.3.4). Reported as
 * the reason of {@code intake.unreachable}, so it carries no names or
 * paths.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum IntakeProblem
{

    /** The server can't be reached, or the connection dropped. */
    NETWORK(false),

    /** The credentials were rejected. */
    AUTH(true),

    /** The account may not list or read the source. */
    PERMISSION(true),

    /** The SFTP server's host key isn't the pinned one. */
    HOST_KEY(true),

    /** The share or directory doesn't exist. */
    NOT_FOUND(true),

    /**
     * The SMB server won't encrypt the session, or gave a guest session;
     * nothing is read without encryption.
     */
    ENCRYPTION(true);

    /** Whether retries wait for the slower auth retry interval. */
    private final boolean slowRetry;

    /**
     * Creates a problem.
     *
     * @param slowRetry whether retries wait for the auth retry interval
     */
    IntakeProblem(boolean slowRetry)
    {
        this.slowRetry = slowRetry;
    }

    /**
     * Tells whether retrying soon is pointless or harmful: a rejected
     * password retried every poll can lock the account.
     *
     * @return true to wait for {@code auth-retry-interval}
     */
    public boolean slowRetry()
    {
        return slowRetry;
    }
}
