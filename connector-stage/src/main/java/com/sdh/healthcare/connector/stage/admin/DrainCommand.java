/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.ControlState;
import com.sdh.healthcare.connector.stage.PauseWatch;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@code drain} (CONTROL-API-DESIGN.md §9): pauses the stage, then waits
 * until its pods hold no live claim, when it can be stopped or upgraded
 * with nothing part way in a pod. The control API starts it and follows
 * it through its record; the CLI waits for it, for at most {@code --wait}.
 *
 * <p>It is drained once the stage has been paused for {@link #SETTLE}, so
 * every pod has read the pause and finished the batch in hand, and no
 * record or item is claimed with a live claim. It fails if the stage is
 * resumed first. It leaves the stage paused: {@code resume} takes work
 * again.
 *
 * @param <C> the stage's context
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class DrainCommand<C extends DrainAdmin>
        implements CliOperation<C, DrainCommand.Request, DrainCommand.Result>,
        Followed<C, DrainCommand.Request, DrainCommand.Result>,
        Answering<DrainCommand.Result>
{

    /**
     * How long a stage is paused before it can be drained: two reads of
     * the pause by every pod, and the batch in hand.
     */
    static final Duration SETTLE = PauseWatch.EVERY.multipliedBy(3);

    /** How long the CLI waits unless asked. */
    static final Duration WAIT = Duration.ofMinutes(10);

    /** A followed drain's parameter: when the stage was paused. */
    private static final String PAUSED_AT = "pausedAt";

    /**
     * What is asked.
     *
     * @param reason  the pause's reason, read when it runs
     * @param waitFor how long the CLI waits for it to be drained
     */
    public record Request(Given<String> reason, Duration waitFor)
    {
    }

    /**
     * Where the drain stands.
     *
     * @param scope   the stage's scope
     * @param paused  its pause
     * @param claims  the live claims left
     * @param drained whether it is drained
     */
    public record Result(String scope, ControlState.Paused paused,
            long claims, boolean drained)
    {
    }

    /** The stage wasn't drained in the time the CLI waited. */
    public static final class NotDrained extends Exception
    {

        /**
         * Creates the exception.
         *
         * @param claims the live claims left
         */
        NotDrained(long claims)
        {
            super(claims + " live claim(s) left; the stage stays paused");
        }
    }

    /** Creates the command. */
    public DrainCommand()
    {
    }

    @Override
    public String name()
    {
        return "drain";
    }

    @Override
    public String usage()
    {
        return """
                drain [--wait <minutes>] < reason.txt
                    Pauses the stage, as pause does, then waits until its
                    pods hold no live claim (10 minutes unless --wait), when
                    it can be stopped or upgraded. Leaves it paused; exits 1
                    if it isn't drained in time.""";
    }

    @Override
    public Set<String> options()
    {
        return Set.of("wait");
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
        return Kind.LONG;
    }

    @Override
    public Request bind(AdminArgs arguments, C context)
    {
        Duration wait = arguments.option("wait").map(DrainCommand::minutes)
                .orElse(WAIT);
        return new Request(() -> ControlStateCommands.reason(
                arguments.input(() -> System.in)), wait);
    }

    /**
     * Reads a number of minutes.
     *
     * @param value the option's value
     * @return the minutes
     * @throws AdminArgs.UsageException if it isn't a whole number from 0
     */
    private static Duration minutes(String value)
    {
        try
        {
            long minutes = Long.parseLong(value);
            if (minutes >= 0)
            {
                return Duration.ofMinutes(minutes);
            }
        }
        catch (NumberFormatException malformed)
        {
            // Said below.
        }
        throw new AdminArgs.UsageException("--wait must be whole minutes,"
                + " 0 or more");
    }

    /**
     * Pauses the stage, and waits until it is drained, for at most the
     * time asked.
     */
    @Override
    public Result run(Request request, C context, Invocation invocation)
            throws Exception
    {
        Result result = start(request, context, invocation);
        long until = System.nanoTime() + request.waitFor().toNanos();
        while (!result.drained() && System.nanoTime() < until)
        {
            Thread.sleep(PauseWatch.EVERY.toMillis());
            Optional<ControlState.Paused> paused =
                    context.controlState(false).paused();
            if (paused.isEmpty())
            {
                throw new IllegalStateException("The stage was resumed"
                        + " before it was drained");
            }
            result = look(context, paused.get());
        }
        invocation.audit().add(Attr.CLAIMS, result.claims());
        return result;
    }

    /** Pauses the stage, and looks once: never waits. */
    @Override
    public Result start(Request request, C context, Invocation invocation)
            throws Exception
    {
        ControlState state = context.controlState(true);
        invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
        String reason = request.reason().get();
        ControlState.Paused paused = state.pause(invocation.principal(),
                reason);
        Result result = look(context, paused);
        invocation.audit().add(Attr.CLAIMS, result.claims());
        return result;
    }

    /**
     * Where the drain stands now.
     *
     * @param context the stage's context
     * @param paused  its pause
     * @return where it stands
     * @throws Exception if the claims can't be counted
     */
    private Result look(C context, ControlState.Paused paused)
            throws Exception
    {
        long claims = context.liveClaims();
        boolean settled = paused.pausedAt() != null && !Instant.now()
                .isBefore(paused.pausedAt().plus(SETTLE));
        return new Result(paused.scope(), paused, claims,
                settled && claims == 0);
    }

    @Override
    public Optional<Map<String, Object>> started(Result result)
    {
        return Optional.of(Answers.object("scope", result.scope(),
                PAUSED_AT, result.paused().pausedAt()));
    }

    /**
     * Where a followed drain stands: done once drained, failed if the
     * stage was resumed.
     */
    @Override
    public Progress progress(Map<String, Object> parameters, C context)
            throws Exception
    {
        Optional<ControlState.Paused> paused =
                context.controlState(false).paused();
        if (paused.isEmpty())
        {
            return new Progress(State.FAILED, Answers.object("resumed",
                    true));
        }
        Result now = look(context, paused.get());
        return new Progress(now.drained() ? State.DONE : State.RUNNING,
                Answers.object("claims", now.claims(), "pausedAt",
                        now.paused().pausedAt(), "settled", now.drained()
                                || !Instant.now().isBefore(now.paused()
                                        .pausedAt().plus(SETTLE))));
    }

    @Override
    public Map<String, Object> answer(Result result)
    {
        Map<String, Object> answer = ControlStateCommands.pauseAnswer(
                result.scope(), Optional.of(result.paused()));
        answer.put("claims", result.claims());
        answer.put("drained", result.drained());
        return answer;
    }

    @Override
    public void print(Result result, PrintStream out) throws Exception
    {
        ControlStateCommands.printPause(result.scope(),
                Optional.of(result.paused()), out);
        if (!result.drained())
        {
            throw new NotDrained(result.claims());
        }
        out.println("Drained: no live claims. It stays paused until"
                + " resume.");
    }
}
