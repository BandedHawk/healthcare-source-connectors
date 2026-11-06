/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.io.PrintStream;
import java.util.Set;

/**
 * One operator command of a stage (design §11.4), such as {@code status}.
 *
 * @param <C> what the stage's commands read from
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface AdminCommand<C>
{

    /**
     * The command's name, as typed after {@code admin}.
     *
     * @return the name, such as {@code status}
     */
    String name();

    /**
     * The command's help: its synopsis, then what it does, indented.
     *
     * @return the text
     */
    String usage();

    /**
     * The options that take a value, without {@code --}.
     *
     * @return the names
     */
    Set<String> options();

    /**
     * The options that take no value, without {@code --}.
     *
     * @return the names
     */
    Set<String> flags();

    /**
     * Runs the command.
     *
     * @param arguments its options
     * @param context   what it reads from
     * @param audit     the audit event, which it adds what it learns to
     * @param out       where its results go
     * @throws Exception if it fails; {@link AdminArgs.UsageException} for a
     *                   usage mistake, {@link StatusParts.Unavailable} when
     *                   what failed is already shown in its output
     */
    void run(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception;
}
