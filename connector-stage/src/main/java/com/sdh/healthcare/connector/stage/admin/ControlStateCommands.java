/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.ControlState;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Every stage's {@code pause}, {@code resume} and {@code control-state}
 * (CONTROL-API-DESIGN.md §9): whether its pods take new work. A pause is
 * kept in the database ({@link ControlState}), under the scope the stage's
 * audit chains are of, and its pods read it within seconds; it outlasts
 * them. Work already taken finishes, and nothing is dropped.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class ControlStateCommands
{

    /** The most bytes of a reason read. */
    private static final int MAX_REASON = 1024;

    /** Not created: holds the commands. */
    private ControlStateCommands()
    {
    }

    /**
     * The commands.
     *
     * @return them
     */
    static List<AdminCommand<AuditTarget>> all()
    {
        return List.of(new Pause(), new Resume(), new State());
    }

    /**
     * Reads a pause's reason: one line, of letters, digits, spaces and a
     * few marks ({@link ControlState#REASON}).
     *
     * @param input where it is read from
     * @return the reason, without its line's end
     * @throws IOException              if it can't be read
     * @throws AdminArgs.UsageException if it is missing or not as allowed
     */
    static String reason(InputStream input) throws IOException
    {
        String text = new String(input.readNBytes(MAX_REASON), UTF_8).strip();
        if (!ControlState.REASON.matcher(text).matches())
        {
            throw new AdminArgs.UsageException("Give the reason on standard"
                    + " input, such as < reason.txt: 1 to 200 letters,"
                    + " digits, spaces or . , : _ # / -");
        }
        return text;
    }

    /**
     * A pause as the API answers it.
     *
     * @param scope  the scope
     * @param paused the pause; empty if it isn't paused
     * @return the answer
     */
    static Map<String, Object> pauseAnswer(String scope,
            Optional<ControlState.Paused> paused)
    {
        return Answers.object("scope", scope, "paused", paused.isPresent(),
                "pausedBy", paused.map(ControlState.Paused::pausedBy)
                        .orElse(null),
                "pausedAt", paused.map(ControlState.Paused::pausedAt)
                        .orElse(null),
                "reason", paused.map(ControlState.Paused::reason)
                        .orElse(null));
    }

    /**
     * Prints a pause.
     *
     * @param scope  the scope
     * @param paused the pause; empty if it isn't paused
     * @param out    where to
     */
    static void printPause(String scope,
            Optional<ControlState.Paused> paused, PrintStream out)
    {
        if (paused.isEmpty())
        {
            out.println(scope + " is not paused");
            return;
        }
        ControlState.Paused pause = paused.get();
        out.println(scope + " is paused, by " + pause.pausedBy() + " at "
                + pause.pausedAt() + ": " + pause.reason());
    }

    /**
     * What the state commands answer.
     *
     * @param scope   the scope
     * @param paused  its pause now; empty if it isn't paused
     * @param changed whether this run changed it
     */
    record Result(String scope, Optional<ControlState.Paused> paused,
            boolean changed)
    {
    }

    /**
     * {@code pause}: the stage's pods stop taking new work. A stage already
     * paused keeps its first pause.
     */
    static final class Pause implements CliOperation<AuditTarget,
            Given<String>, Result>, Answering<Result>
    {

        /** Creates the command. */
        Pause()
        {
        }

        @Override
        public String name()
        {
            return "pause";
        }

        @Override
        public String usage()
        {
            return """
                    pause < reason.txt
                        Stops the stage's pods taking new work, within
                        seconds: its input, a reprocess run's queueing, its
                        corrections; a landing node's listing or pushes.
                        Work already taken finishes and commits, and leases
                        are still renewed. Kept until resume, whatever pods
                        start or stop. The reason, one line, is kept with
                        who paused and when.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of();
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.PIPELINE_OPERATE;
        }

        @Override
        public Kind kind()
        {
            return Kind.ACTION;
        }

        @Override
        public Given<String> bind(AdminArgs arguments, AuditTarget context)
        {
            return () -> reason(arguments.input(() -> System.in));
        }

        @Override
        public Result run(Given<String> reason, AuditTarget context,
                Invocation invocation) throws Exception
        {
            ControlState state = context.controlState(true);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            String text = reason.get();
            Optional<ControlState.Paused> before = state.paused();
            invocation.audit().add(Attr.PAUSED, before.isPresent());
            ControlState.Paused now = state.pause(invocation.principal(),
                    text);
            return new Result(state.scope(), Optional.of(now),
                    before.isEmpty());
        }

        @Override
        public Map<String, Object> answer(Result result)
        {
            Map<String, Object> answer = pauseAnswer(result.scope(),
                    result.paused());
            answer.put("changed", result.changed());
            return answer;
        }

        @Override
        public void print(Result result, PrintStream out)
        {
            if (!result.changed())
            {
                out.print("Already paused. ");
            }
            ControlStateCommands.printPause(result.scope(), result.paused(),
                    out);
        }
    }

    /** {@code resume}: the stage's pods take work again. */
    static final class Resume implements CliOperation<AuditTarget,
            Resume.Request, Result>, Answering<Result>
    {

        /** What is asked: nothing but the resume. */
        record Request()
        {
        }

        /** Creates the command. */
        Resume()
        {
        }

        @Override
        public String name()
        {
            return "resume";
        }

        @Override
        public String usage()
        {
            return """
                    resume
                        Clears the stage's pause: its pods take work again
                        within seconds. A stage not paused is left as it
                        is.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of();
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.PIPELINE_OPERATE;
        }

        @Override
        public Kind kind()
        {
            return Kind.ACTION;
        }

        @Override
        public Request bind(AdminArgs arguments, AuditTarget context)
        {
            return new Request();
        }

        @Override
        public Result run(Request request, AuditTarget context,
                Invocation invocation)
        {
            ControlState state = context.controlState(true);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            Optional<ControlState.Paused> cleared = state.resume();
            invocation.audit().add(Attr.PAUSED, cleared.isPresent());
            cleared.ifPresent(pause -> invocation.audit().add(
                    Attr.REQUESTED_BY, pause.pausedBy()));
            return new Result(state.scope(), Optional.empty(),
                    cleared.isPresent());
        }

        @Override
        public Map<String, Object> answer(Result result)
        {
            Map<String, Object> answer = pauseAnswer(result.scope(),
                    result.paused());
            answer.put("changed", result.changed());
            return answer;
        }

        @Override
        public void print(Result result, PrintStream out)
        {
            out.println(result.changed() ? "Resumed " + result.scope()
                    : result.scope() + " was not paused");
        }
    }

    /** {@code control-state}: whether the stage is paused, by whom. */
    static final class State implements CliOperation<AuditTarget,
            State.Request, Result>, Answering<Result>
    {

        /** What is asked: nothing but the state. */
        record Request()
        {
        }

        /** Creates the command. */
        State()
        {
        }

        @Override
        public String name()
        {
            return "control-state";
        }

        @Override
        public String usage()
        {
            return """
                    control-state
                        Whether the stage is paused, by whom, since when,
                        and why. Changes nothing.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of();
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.PIPELINE_VIEW;
        }

        @Override
        public Kind kind()
        {
            return Kind.QUERY;
        }

        @Override
        public Request bind(AdminArgs arguments, AuditTarget context)
        {
            return new Request();
        }

        @Override
        public Result run(Request request, AuditTarget context,
                Invocation invocation)
        {
            ControlState state = context.controlState(false);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            Optional<ControlState.Paused> paused = state.paused();
            invocation.audit().add(Attr.PAUSED, paused.isPresent());
            return new Result(state.scope(), paused, false);
        }

        @Override
        public Map<String, Object> answer(Result result)
        {
            return pauseAnswer(result.scope(), result.paused());
        }

        @Override
        public void print(Result result, PrintStream out)
        {
            ControlStateCommands.printPause(result.scope(), result.paused(),
                    out);
        }
    }
}
