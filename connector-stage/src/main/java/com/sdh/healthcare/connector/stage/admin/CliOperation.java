/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.io.PrintStream;
import java.util.Map;

/**
 * An operation with its CLI binding: the command's options bound to the
 * operation's request, and its result printed (CONTROL-API-DESIGN.md §3).
 * The command's text is the CLI's alone; the control API gives the same
 * result as structured rows.
 *
 * @param <C> what it reads from: the stage's context
 * @param <Q> its request
 * @param <R> its result
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface CliOperation<C, Q, R>
        extends Operation<C, Q, R>, AdminCommand<C>
{

    /**
     * Binds the command's options to a request. It reads nothing from the
     * context but what is {@link Given} to be read later, such as standard
     * input.
     *
     * @param arguments the command's options
     * @param context   the stage's context
     * @return the request
     * @throws AdminArgs.UsageException if an option is malformed
     */
    Q bind(AdminArgs arguments, C context);

    /**
     * Prints a result.
     *
     * @param result the result
     * @param out    where it goes
     * @throws Exception if the result says the run failed after all, as
     *                   {@link StatusParts.Unavailable} does when a part
     *                   couldn't be read
     */
    void print(R result, PrintStream out) throws Exception;

    /**
     * Whether the command only reads, and reads no PHI: a query.
     *
     * @return true for a query
     */
    @Override
    default boolean readsOnly()
    {
        return kind() == Kind.QUERY;
    }

    @Override
    default void run(AdminArgs arguments, C context, AuditRecord audit,
            PrintStream out) throws Exception
    {
        Q request = bind(arguments, context);
        R result = run(request, context, new Invocation(
                Caller.cli(audit.operator()), audit, sink(out)));
        print(result, out);
    }

    /**
     * The CLI's sink: rows as JSON lines, sealed parts on lines starting
     * {@link ExportSealer#LINE_PREFIX}.
     *
     * @param out where they go
     * @return the sink
     */
    static Invocation.Sink sink(PrintStream out)
    {
        return new Invocation.Sink()
        {
            @Override
            public void row(Map<String, Object> row)
            {
                out.println(JSONObjectUtils.toJSONString(row));
            }

            @Override
            public void sealed(String jwe)
            {
                out.println(ExportSealer.LINE_PREFIX + jwe);
            }
        };
    }
}
