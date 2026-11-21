/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.control;

/**
 * A control API operation's refusal with a status and code of its own,
 * such as {@code 403 not-for-yourself}, rather than the {@code 409
 * refused} any other refusal is answered with. Its detail is what to do
 * about it, and never holds stored data.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Refusal extends RuntimeException
{

    /** Serializable, as exceptions are. */
    private static final long serialVersionUID = 1L;

    /** The HTTP status. */
    private final int status;

    /** The refusal's code, as its problem's title. */
    private final String code;

    /**
     * Creates the refusal.
     *
     * @param status the HTTP status
     * @param code   the code, such as {@code not-for-yourself}
     * @param detail what to do about it
     */
    public Refusal(int status, String code, String detail)
    {
        super(detail);
        this.status = status;
        this.code = code;
    }

    /**
     * The HTTP status.
     *
     * @return it
     */
    public int status()
    {
        return status;
    }

    /**
     * The refusal's code.
     *
     * @return it
     */
    public String code()
    {
        return code;
    }
}
