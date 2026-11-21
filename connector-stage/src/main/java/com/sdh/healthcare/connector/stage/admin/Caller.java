/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

/**
 * Who runs an operation, and how that was proved (CONTROL-API-DESIGN.md
 * §2). From the control API, the authenticated principal; from the CLI,
 * the operator's name as given, which only the platform's audit of
 * {@code exec} proves (HIPAA.md §8.1 item 3).
 *
 * @param principal the caller's name, recorded as {@code sdh.operator}
 * @param auth      how it was proved
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Caller(String principal, Auth auth)
{

    /** How a caller's name was proved. */
    public enum Auth
    {

        /** Given to the CLI: self-asserted. */
        CLI,

        /** An OIDC bearer token, checked. */
        JWT,

        /** A client certificate, checked. */
        MTLS
    }

    /**
     * The operator of a CLI run.
     *
     * @param operator the name given
     * @return the caller
     */
    public static Caller cli(String operator)
    {
        return new Caller(operator, Auth.CLI);
    }
}
