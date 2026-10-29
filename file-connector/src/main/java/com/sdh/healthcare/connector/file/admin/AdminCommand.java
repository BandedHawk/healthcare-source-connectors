package com.sdh.healthcare.connector.file.admin;

import java.io.PrintStream;
import java.util.Set;

/** One operator command (design §11.4). */
interface AdminCommand {
    String name();

    /** Every option and flag the command accepts. */
    Set<String> options();

    /** The subset of {@link #options()} that take no value. */
    Set<String> flags();

    void run(AdminArgs args, AdminContext ctx, AuditRecord audit, PrintStream out) throws Exception;
}
