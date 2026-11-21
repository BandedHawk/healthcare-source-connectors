/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.util.EnumSet;
import java.util.Set;

/**
 * An operator's operation on a stage, apart from how it is called
 * (CONTROL-API-DESIGN.md §3): a request in, a result out, both free of
 * PHI except what is sealed, with the permission it needs and what kind of
 * operation it is. The CLI ({@link CliOperation}) and the control API run
 * the same operations, and write the same audit events.
 *
 * @param <C> what it reads from: the stage's context
 * @param <Q> its request
 * @param <R> its result
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface Operation<C, Q, R>
{

    /** What an operation does, which decides how it is called. */
    enum Kind
    {

        /** Reads, and reads no PHI: a viewer's. */
        QUERY,

        /** Changes something, and answers at once. */
        ACTION,

        /** Changes something, and goes on after it answers. */
        LONG,

        /** Reads PHI, and gives it only sealed. */
        SEALED
    }

    /** Whether a run needs a second person's approval (§6). */
    enum Approval
    {

        /** One caller is enough. */
        NONE,

        /** A second caller, holding the same permission, approves. */
        FOUR_EYES
    }

    /**
     * The operation's name, which is its CLI command's.
     *
     * @return the name, such as {@code stuck}
     */
    String name();

    /**
     * The permission it needs for its usual request.
     *
     * @return the permission
     */
    Permission permission();

    /**
     * The permissions a request needs: the usual one, unless the request
     * asks for more or less (a plaintext restore, a status of a run).
     *
     * @param request the request
     * @return the permissions, all of which are needed
     */
    default Set<Permission> permissions(Q request)
    {
        return EnumSet.of(permission());
    }

    /**
     * What kind of operation it is.
     *
     * @return the kind
     */
    Kind kind();

    /**
     * Whether a request needs a second person's approval.
     *
     * @param request the request
     * @return the approval needed; none by default
     */
    default Approval approval(Q request)
    {
        return Approval.NONE;
    }

    /**
     * Runs the operation.
     *
     * @param request    what is asked
     * @param context    what it reads from
     * @param invocation who runs it, its audit event and its sink
     * @return the result
     * @throws Exception if it fails; {@link AdminArgs.UsageException} when
     *                   the request is wrong
     */
    R run(Q request, C context, Invocation invocation) throws Exception;
}
