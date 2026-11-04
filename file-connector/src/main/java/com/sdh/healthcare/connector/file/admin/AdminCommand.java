/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import java.io.PrintStream;
import java.util.Set;

/**
 * One operator command (design §11.4).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
interface AdminCommand
{

    /**
     * The command's name on the command line.
     *
     * @return the name
     */
    String name();

    /**
     * Every option and flag the command accepts.
     *
     * @return the option names, without {@code --}
     */
    Set<String> options();

    /**
     * The subset of {@link #options()} that take no value.
     *
     * @return the flag names, without {@code --}
     */
    Set<String> flags();

    /**
     * Runs the command.
     *
     * @param arguments the parsed command line
     * @param context the configuration, keys and tables
     * @param audit the audit event to fill in as the command goes
     * @param out where to print the result
     * @throws Exception if the command fails
     */
    void run(AdminArgs arguments, AdminContext context, AuditRecord audit,
            PrintStream out) throws Exception;
}
