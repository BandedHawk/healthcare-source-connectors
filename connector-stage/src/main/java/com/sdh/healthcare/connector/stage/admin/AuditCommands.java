/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.audit.AuditTrail;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Every stage's commands on its durable audit journal (HIPAA.md §3.1,
 * audit controls): {@code audit-verify} checks it for changes and gaps,
 * {@code audit-export} writes it out for the operator's SIEM, and
 * {@code audit-prune} removes chains older than the operator's retention.
 * The journal holds no PHI, so neither does their output.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class AuditCommands
{

    /**
     * How long an unclosed chain may go unwritten before {@code
     * audit-verify} counts its process as stopped abruptly, by default.
     */
    static final Duration STALE = Duration.ofMinutes(10);

    /** Not instantiated. */
    private AuditCommands()
    {
    }

    /**
     * The commands.
     *
     * @return them
     */
    static List<AdminCommand<AuditTarget>> all()
    {
        return List.of(new Verify(), new Export(), new Prune());
    }

    /**
     * Reads a time: an instant, such as {@code 2026-11-20T09:00:00Z}, or
     * a date, which is its start in UTC.
     *
     * @param name  the option, for errors
     * @param value its value
     * @return the time
     * @throws AdminArgs.UsageException if it is neither
     */
    static Instant time(String name, String value)
    {
        try
        {
            return value.contains("T") ? Instant.parse(value)
                    : LocalDate.parse(value).atStartOfDay(ZoneOffset.UTC)
                            .toInstant();
        }
        catch (DateTimeParseException malformed)
        {
            throw new AdminArgs.UsageException("--" + name + " must be a date"
                    + " (2026-11-20) or an instant (2026-11-20T09:00:00Z)");
        }
    }

    /**
     * An optional time option.
     *
     * @param arguments the options
     * @param name      the option
     * @return the time; empty if not given
     */
    private static Optional<Instant> time(AdminArgs arguments, String name)
    {
        return arguments.option(name).map(value -> time(name, value));
    }

    /** {@code audit-verify}. */
    static final class Verify implements CliOperation<AuditTarget,
            Verify.Request, Verify.Result>
    {

        /**
         * What is asked.
         *
         * @param from  the span's start; null for the first chain
         * @param to    its end; null for now
         * @param stale how long unwritten before an open chain is a
         *              warning
         */
        record Request(Instant from, Instant to, Duration stale)
        {
        }

        /**
         * What was found.
         *
         * @param found  each chain, with what is wrong with it
         * @param sealed whether seals were checked: false with no public
         *               signing key configured
         */
        record Result(AuditTrail.Verification found, boolean sealed)
        {
        }

        /** Creates the command. */
        Verify()
        {
        }

        @Override
        public String name()
        {
            return "audit-verify";
        }

        @Override
        public String usage()
        {
            return """
                    audit-verify [--from <date|instant>] [--to <...>]
                                 [--stale <minutes>]
                        Checks the audit journal's chains written in the
                        span (all by default), each from its start: every
                        entry's hash and number, each chain's head, and
                        every seal against the stage's signing key. Shows
                        each chain with what is wrong: a gap, a broken or
                        rewritten entry, a missing head or a bad seal mean
                        the journal was changed (exit status 1); entries
                        after the last seal, or a chain whose process
                        stopped without closing it (not written for
                        --stale minutes, 10), are warnings. Reads only.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("from", "to", "stale");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.AUDIT_READ;
        }

        @Override
        public Kind kind()
        {
            return Kind.QUERY;
        }

        @Override
        public Request bind(AdminArgs arguments, AuditTarget context)
        {
            return new Request(time(arguments, "from").orElse(null),
                    time(arguments, "to").orElse(null),
                    arguments.option("stale").map(Verify::minutes)
                            .orElse(STALE));
        }

        @Override
        public Result run(Request request, AuditTarget context,
                Invocation invocation) throws Exception
        {
            Optional<AuditTrail.SealKey> key = context.sealKey();
            AuditTrail.Verification found = context.trail().verify(
                    request.from(), request.to(), key.orElse(null),
                    Instant.now(), request.stale());
            long tampered = found.chains().stream()
                    .filter(AuditTrail.ChainReport::tampered).count();
            invocation.audit().add(Attr.COUNT, found.entries());
            invocation.audit().add(Attr.OUTCOME, tampered == 0 ? "intact"
                    : "changed");
            return new Result(found, key.isPresent());
        }

        /**
         * Prints each chain with what is wrong with it.
         *
         * @param result what was found
         * @param out    where to
         * @throws StatusParts.Unavailable if a chain was changed, so the
         *                                 exit status is 1
         */
        @Override
        public void print(Result result, PrintStream out)
                throws StatusParts.Unavailable
        {
            AuditTrail.Verification found = result.found();
            for (AuditTrail.ChainReport chain : found.chains())
            {
                out.println(chain.chainId() + " " + chain.source() + " pod="
                        + chain.pod() + " started=" + chain.startedAt()
                        + (chain.closedAt() == null ? " open"
                                : " closed=" + chain.closedAt())
                        + " entries=" + chain.entries() + " seals="
                        + chain.seals()
                        + (chain.sealedUpTo() < 0 ? ""
                                : " sealed-up-to=" + chain.sealedUpTo()));
                for (AuditTrail.Problem problem : chain.problems())
                {
                    out.println("    " + (problem.kind().tampering()
                            ? "CHANGED " : "warning ") + problem.kind()
                            + " at " + problem.seq()
                            + (problem.count() > 1
                                    ? " (" + problem.count() + ")" : ""));
                }
            }
            long tampered = found.chains().stream()
                    .filter(AuditTrail.ChainReport::tampered).count();
            out.println(found.chains().size() + " chains, " + found.entries()
                    + " entries"
                    + (!result.sealed() ? "; seals not checked: no public"
                            + " signing key configured" : "")
                    + "; " + (tampered == 0 ? "no changes found"
                            : tampered + " chains changed"));
            if (tampered > 0)
            {
                throw new StatusParts.Unavailable();
            }
        }

        /**
         * Reads {@code --stale}.
         *
         * @param value the option's value
         * @return the duration
         * @throws AdminArgs.UsageException if it isn't a whole number of
         *                                  minutes, at least 1
         */
        private static Duration minutes(String value)
        {
            try
            {
                long minutes = Long.parseLong(value);
                if (minutes >= 1)
                {
                    return Duration.ofMinutes(minutes);
                }
            }
            catch (NumberFormatException malformed)
            {
                // reported below
            }
            throw new AdminArgs.UsageException(
                    "--stale must be a whole number of minutes, at least 1");
        }
    }

    /** {@code audit-export}. */
    static final class Export implements CliOperation<AuditTarget,
            Export.Request, Long>
    {

        /**
         * What is asked.
         *
         * @param from the span's start; null for the first event
         * @param to   its end; null for now
         */
        record Request(Instant from, Instant to)
        {
        }

        /** Creates the command. */
        Export()
        {
        }

        @Override
        public String name()
        {
            return "audit-export";
        }

        @Override
        public String usage()
        {
            return """
                    audit-export [--from <date|instant>] [--to <...>]
                        Prints the audit journal's events emitted in the
                        span (all by default), oldest first, one JSON
                        object a line: chain, seq, at, event, severity,
                        attributes and the entry's hash, for a SIEM or an
                        archive. Events hold no PHI. Reads only.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("from", "to");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.AUDIT_READ;
        }

        @Override
        public Kind kind()
        {
            return Kind.QUERY;
        }

        @Override
        public Request bind(AdminArgs arguments, AuditTarget context)
        {
            return new Request(time(arguments, "from").orElse(null),
                    time(arguments, "to").orElse(null));
        }

        /**
         * Streams the events to the sink, one row each.
         *
         * @return how many
         */
        @Override
        public Long run(Request request, AuditTarget context,
                Invocation invocation) throws Exception
        {
            HexFormat hex = HexFormat.of();
            long count = context.trail().export(request.from(),
                    request.to(), event ->
                    {
                        Map<String, Object> line = new LinkedHashMap<>();
                        line.put("chain", event.chainId().toString());
                        line.put("seq", event.seq());
                        line.put("at", event.at().toString());
                        line.put("event", event.name());
                        line.put("severity", event.severity());
                        line.put("attributes", event.attributes());
                        line.put("hash", hex.formatHex(event.hash()));
                        invocation.sink().row(line);
                    });
            invocation.audit().add(Attr.COUNT, count);
            return count;
        }

        /**
         * Prints nothing more: the events went to the sink.
         *
         * @param result how many
         * @param out    unused
         */
        @Override
        public void print(Long result, PrintStream out)
        {
        }
    }

    /** {@code audit-prune}. */
    static final class Prune implements CliOperation<AuditTarget,
            Prune.Request, Prune.Result>
    {

        /**
         * What is asked.
         *
         * @param before the time the chains pruned were last written before
         */
        record Request(Instant before)
        {
        }

        /**
         * What was done.
         *
         * @param before the time asked for
         * @param pruned the chains and entries deleted
         */
        record Result(Instant before, AuditTrail.Pruned pruned)
        {
        }

        /** Creates the command. */
        Prune()
        {
        }

        @Override
        public String name()
        {
            return "audit-prune";
        }

        @Override
        public String usage()
        {
            return """
                    audit-prune --before <date|instant>
                        Deletes the audit journal's chains last written
                        before the time, whole, with their entries and
                        seals: for the retention the operator has chosen
                        for audit logs. Export them first if they must be
                        kept elsewhere. What is left still verifies.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("before");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.AUDIT_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.LONG;
        }

        /**
         * A second person approves (CONTROL-API-DESIGN.md §6).
         *
         * @param request the request
         * @return four eyes
         */
        @Override
        public Approval approval(Request request)
        {
            return Approval.FOUR_EYES;
        }

        @Override
        public Request bind(AdminArgs arguments, AuditTarget context)
        {
            return new Request(time("before", arguments.required("before")));
        }

        @Override
        public Result run(Request request, AuditTarget context,
                Invocation invocation) throws Exception
        {
            Instant before = request.before();
            if (before.isAfter(Instant.now().minus(STALE)))
            {
                throw new AdminArgs.UsageException("--before must be at least"
                        + " " + STALE.toMinutes() + " minutes ago, so no"
                        + " chain still being written is pruned");
            }
            AuditTrail.Pruned pruned = context.trailToChange().prune(before);
            invocation.audit().add(Attr.COUNT, pruned.entries());
            return new Result(before, pruned);
        }

        @Override
        public void print(Result result, PrintStream out)
        {
            out.println("Pruned " + result.pruned().chains() + " chains, "
                    + result.pruned().entries()
                    + " entries, last written before " + result.before());
        }
    }
}
