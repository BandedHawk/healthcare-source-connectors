/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

/**
 * What a caller must be granted to run an operation (CONTROL-API.md,
 * CONTROL-API-DESIGN.md §4). Each {@link Operation} names the permissions
 * it needs; the control API checks them against the caller's roles, as
 * the customer's settings map them. The CLI, run through {@code kubectl
 * exec}, is guarded by the platform's access control instead.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public enum Permission
{

    /** Sees the pipeline's state: never PHI, never a change. */
    PIPELINE_VIEW("pipeline.view"),

    /** Acts on the pipeline: pause, reprocess, failures, resubmit. */
    PIPELINE_OPERATE("pipeline.operate"),

    /** Reads PHI, sealed to the caller's registered key. */
    PHI_EXPORT("phi.export"),

    /** Reads PHI in plaintext, with a ticket and a second person. */
    PHI_BREAK_GLASS("phi.break-glass"),

    /** Reads and checks the audit journal. */
    AUDIT_READ("audit.read"),

    /** Deletes old chains of the audit journal. */
    AUDIT_ADMIN("audit.admin"),

    /** Rotates keys and re-keys stored data. */
    KEYS_ADMIN("keys.admin"),

    /** Sets retention, purges items and erases a pipeline. */
    DATA_ADMIN("data.admin");

    /** The permission's name, as the customer's settings give it. */
    private final String key;

    /**
     * Names a permission.
     *
     * @param key its name in settings
     */
    Permission(String key)
    {
        this.key = key;
    }

    /**
     * The permission's name, as the customer's settings give it.
     *
     * @return the name, such as {@code pipeline.view}
     */
    public String key()
    {
        return key;
    }
}
