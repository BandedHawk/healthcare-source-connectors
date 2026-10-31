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
interface AdminCommand {
    String name();

    /** Every option and flag the command accepts. */
    Set<String> options();

    /** The subset of {@link #options()} that take no value. */
    Set<String> flags();

    void run(AdminArgs args, AdminContext ctx, AuditRecord audit, PrintStream out) throws Exception;
}
