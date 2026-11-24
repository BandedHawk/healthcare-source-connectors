/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.Payload;
import com.nimbusds.jose.util.JSONObjectUtils;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.WrappedRows;
import com.sdh.healthcare.connector.core.maintenance.Rekey;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.ControlLog;
import com.sdh.healthcare.connector.stage.ControlState;
import com.sdh.healthcare.connector.stage.PurgeRequests;
import com.sdh.healthcare.connector.stage.RetentionPolicy;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * Every stage's data commands (CONTROL-API-DESIGN.md §11, HIPAA.md P4):
 * {@code retention} and {@code retention-set}, how long the stage keeps its
 * rows, within the bounds its settings give; {@code purge}, an item's rows
 * removed from every table the stage writes, and {@code purges}, where
 * those asked for stand; and {@code erase}, every table and row the stage
 * keeps removed at the end of a contract, with a signed certificate.
 * Purge and erase each need a second person's approval and a ticket. None
 * of them destroys a KEK: that is the key custodian's act, in the KMS.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class DataCommands
{

    /** A ticket: a reference to a change or incident, never free text. */
    static final Pattern TICKET = Pattern.compile("[A-Za-z0-9._:/-]{1,64}");

    /** The longest retention, in days, that can be asked for: 100 years. */
    static final long MAX_DAYS = 36_525;

    /** The erasure certificate's JOSE type. */
    public static final String CERTIFICATE_TYPE = "sdh-erasure+jwt";

    /** Not created: holds the commands. */
    private DataCommands()
    {
    }

    /**
     * The commands.
     *
     * @param <C> the stage's context
     * @return them
     */
    public static <C extends DataAdmin> List<AdminCommand<C>> all()
    {
        return List.of(new Retention<>(), new RetentionSet<>(),
                new Purge<>(), new Purges<>(), new Erase<>());
    }

    /**
     * Reads a ticket.
     *
     * @param arguments the arguments
     * @return the ticket
     * @throws AdminArgs.UsageException if there is none, or it isn't a
     *                                  reference
     */
    static String ticket(AdminArgs arguments)
    {
        String ticket = arguments.option("ticket").orElseThrow(() ->
                new AdminArgs.UsageException("--ticket is required: the"
                        + " change or incident it is for"));
        if (!TICKET.matcher(ticket).matches())
        {
            throw new AdminArgs.UsageException("--ticket must be a"
                    + " reference: 1 to 64 letters, digits or . _ : / -");
        }
        return ticket;
    }

    /**
     * The operation a run is part of: the API's, so a run taken over asks
     * for the same; else a new one, the CLI's own, added to the audit.
     *
     * @param invocation the run
     * @return its ID
     */
    static String operationId(Invocation invocation)
    {
        String id = invocation.audit().fields().toStrings()
                .get(Attr.OPERATION_ID.key());
        if (id == null)
        {
            id = UUID.randomUUID().toString();
            invocation.audit().add(Attr.OPERATION_ID, id);
        }
        return id;
    }

    /**
     * A retention in days, as answered.
     *
     * @param keep the retention; empty for forever
     * @return its days; null for forever
     */
    static Long days(Optional<Duration> keep)
    {
        return keep.map(Duration::toDays).orElse(null);
    }

    /**
     * A retention, as printed.
     *
     * @param keep the retention; empty for forever
     * @return it
     */
    static String text(Optional<Duration> keep)
    {
        return keep.map(each -> each.toDays() + " day(s)").orElse("forever");
    }

    /**
     * A scope's retention.
     *
     * @param scope   the scope
     * @param inForce the retention maintenance applies
     * @param setting the setting's
     * @param set     the one an operator set; empty if none was
     * @param minimum the shortest allowed
     * @param maximum the longest allowed; empty if forever is
     */
    public record RetentionState(String scope, Optional<Duration> inForce,
            Optional<Duration> setting, Optional<ControlState.Retention> set,
            Duration minimum, Optional<Duration> maximum)
    {

        /**
         * Reads a scope's retention.
         *
         * @param state  the scope's state
         * @param policy the stage's policy
         * @return it
         */
        static RetentionState of(ControlState state, RetentionPolicy policy)
        {
            return new RetentionState(state.scope(), policy.inForce(state),
                    policy.setting(), state.retention(), policy.minimum(),
                    policy.maximum());
        }

        /**
         * The answer.
         *
         * @return it
         */
        Map<String, Object> answer()
        {
            return Answers.object("scope", scope,
                    "days", days(inForce), "forever", inForce.isEmpty(),
                    "settingDays", days(setting),
                    "setBy", set.map(ControlState.Retention::setBy)
                            .orElse(null),
                    "setAt", set.map(ControlState.Retention::setAt)
                            .orElse(null),
                    "minimumDays", minimum.toDays(),
                    "maximumDays", days(maximum));
        }

        /**
         * Prints it.
         *
         * @param out where to
         */
        void print(PrintStream out)
        {
            out.println(scope + " keeps rows " + text(inForce)
                    + set.map(each -> ", set by " + each.setBy() + " at "
                            + each.setAt())
                            .orElse(", its setting's")
                    + "; allowed " + minimum.toDays() + " day(s) to "
                    + maximum.map(each -> each.toDays() + " day(s)")
                            .orElse("forever"));
        }
    }

    /**
     * {@code retention}: how long the stage keeps its rows.
     *
     * @param <C> the stage's context
     */
    static final class Retention<C extends DataAdmin> implements
            CliOperation<C, Retention.Request, RetentionState>,
            Answering<RetentionState>
    {

        /** What is asked: nothing but the retention. */
        record Request()
        {
        }

        /** Creates the command. */
        Retention()
        {
        }

        @Override
        public String name()
        {
            return "retention";
        }

        @Override
        public String usage()
        {
            return """
                    retention
                        How long the stage keeps its rows: the retention its
                        maintenance applies, whether an operator set it or it
                        is its setting's, and the bounds it may be set
                        within. Changes nothing.""";
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
            return Permission.DATA_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.QUERY;
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request();
        }

        @Override
        public RetentionState run(Request request, C context,
                Invocation invocation) throws Exception
        {
            ControlState state = context.controlState(false);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            return RetentionState.of(state, context.retentionPolicy());
        }

        @Override
        public Map<String, Object> answer(RetentionState result)
        {
            return result.answer();
        }

        @Override
        public void print(RetentionState result, PrintStream out)
        {
            result.print(out);
        }
    }

    /**
     * {@code retention-set}: how long the stage keeps its rows from its
     * next maintenance run, within its bounds.
     *
     * @param <C> the stage's context
     */
    static final class RetentionSet<C extends DataAdmin> implements
            CliOperation<C, RetentionSet.Request, RetentionState>,
            Answering<RetentionState>
    {

        /**
         * What is asked.
         *
         * @param keep how long; empty keeps rows forever
         */
        record Request(Optional<Duration> keep)
        {
        }

        /** Creates the command. */
        RetentionSet()
        {
        }

        @Override
        public String name()
        {
            return "retention-set";
        }

        @Override
        public String usage()
        {
            return """
                    retention-set --days <n> | --forever
                        Keeps the stage's rows this many days, or forever,
                        from its next maintenance run: older rows are then
                        removed, and can't be brought back. Refused outside
                        the bounds sdh.maintenance.retention-bounds gives,
                        and below the stage's own floor.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("days");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of("forever");
        }

        @Override
        public Permission permission()
        {
            return Permission.DATA_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.ACTION;
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            Optional<String> days = arguments.option("days");
            boolean forever = arguments.flag("forever");
            if (days.isPresent() == forever)
            {
                throw new AdminArgs.UsageException("Give --days or"
                        + " --forever");
            }
            if (forever)
            {
                return new Request(Optional.empty());
            }
            try
            {
                long count = Long.parseLong(days.get());
                if (count > 0 && count <= MAX_DAYS)
                {
                    return new Request(Optional.of(Duration.ofDays(count)));
                }
            }
            catch (NumberFormatException malformed)
            {
                // Said below.
            }
            throw new AdminArgs.UsageException("--days must be whole days,"
                    + " 1 to " + MAX_DAYS);
        }

        @Override
        public RetentionState run(Request request, C context,
                Invocation invocation) throws Exception
        {
            ControlState state = context.controlState(true);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            RetentionPolicy policy = context.retentionPolicy();
            try
            {
                policy.check(request.keep());
            }
            catch (IllegalArgumentException outside)
            {
                throw new AdminArgs.UsageException(outside.getMessage());
            }
            ControlState.Retention set = state.setRetention(
                    invocation.principal(), request.keep());
            invocation.audit().add(Attr.RETENTION, set.wire());
            ControlChanges.keep(context, ControlLog.Kind.RETENTION,
                    operationId(invocation), invocation.principal(),
                    Map.of("retention", set.wire()));
            return RetentionState.of(state, policy);
        }

        @Override
        public Map<String, Object> answer(RetentionState result)
        {
            return result.answer();
        }

        @Override
        public void print(RetentionState result, PrintStream out)
        {
            result.print(out);
        }
    }

    /**
     * A purge as the API answers it.
     *
     * @param purge the purge
     * @return the answer
     */
    static Map<String, Object> purgeAnswer(PurgeRequests.Purge purge)
    {
        return Answers.object("id", purge.id(), "scope", purge.scope(),
                "itemId", purge.itemId().toString(),
                "state", purge.state().wire(),
                "requestedBy", purge.requestedBy(),
                "requestedAt", purge.requestedAt(), "ticket", purge.ticket(),
                "files", purge.files(), "rows", purge.rows(),
                "failure", purge.failure(), "updatedAt", purge.updatedAt());
    }

    /**
     * Prints a purge.
     *
     * @param purge the purge
     * @param out   where to
     */
    static void printPurge(PurgeRequests.Purge purge, PrintStream out)
    {
        out.println("Purge " + purge.id() + " of item " + purge.itemId()
                + ": " + purge.state().wire() + ", " + purge.files()
                + " file(s), " + purge.rows() + " row(s)"
                + (purge.failure() == null ? "" : ", " + purge.failure())
                + "; asked by " + purge.requestedBy() + " at "
                + purge.requestedAt() + " under " + purge.ticket());
    }

    /**
     * {@code purge}: removes an item's rows from every table the stage
     * writes, carried out by its maintenance, on the pod holding its
     * duties ({@code PurgePass}). It needs a second person's approval and
     * a ticket. Its run asks for it, then waits until it is done or
     * failed; the CLI's for at most {@code --wait}.
     *
     * @param <C> the stage's context
     */
    static final class Purge<C extends DataAdmin> implements
            CliOperation<C, Purge.Request, PurgeRequests.Purge>,
            Answering<PurgeRequests.Purge>
    {

        /**
         * What is asked.
         *
         * @param item    the item
         * @param ticket  the ticket it is asked under
         * @param waitFor how long to wait for it; null to wait until it is
         *                done or failed
         */
        record Request(UUID item, String ticket, Duration waitFor)
        {
        }

        /** Creates the command. */
        Purge()
        {
        }

        @Override
        public String name()
        {
            return "purge";
        }

        @Override
        public String usage()
        {
            return """
                    purge --item-id <uuid> --ticket <ref> [--wait <min>]
                        Removes every row of the item from the tables the
                        stage writes, rewriting the files that held them
                        and deleting those files, and deletes its
                        corrections. Its failures are kept: they hold no
                        document values. The stage's maintenance does it, on
                        the pod holding its duties; purges shows how far it
                        is. Waits until it is done or failed, or for --wait
                        minutes. Copies delivered out of the pipeline are
                        the receiver's to purge. Needs a second person's
                        approval through the API, and a ticket.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("item-id", "ticket", "wait");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.DATA_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.LONG;
        }

        @Override
        public Approval approval(Request request)
        {
            return Approval.FOUR_EYES;
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            UUID item;
            try
            {
                item = UUID.fromString(arguments.required("item-id"));
            }
            catch (IllegalArgumentException malformed)
            {
                throw new AdminArgs.UsageException("--item-id must be a"
                        + " UUID");
            }
            Duration wait = arguments.option("wait")
                    .map(KeyCommands::minutes).orElse(null);
            return new Request(item, ticket(arguments), wait);
        }

        /**
         * Asks for the purge, and waits until it is done or failed, for at
         * most the time asked. A run taken over by another control pod
         * finds the one it asked for, and waits on it.
         *
         * @throws IllegalStateException if it fails
         */
        @Override
        public PurgeRequests.Purge run(Request request, C context,
                Invocation invocation) throws Exception
        {
            PurgeRequests requests = context.purgeRequests(true);
            invocation.audit().add(Attr.CONTROL_SCOPE, requests.scope());
            invocation.audit().add(Attr.ITEM_ID, request.item().toString());
            invocation.audit().add(Attr.TICKET, request.ticket());
            String id = operationId(invocation);
            PurgeRequests.Purge now = requests.request(id,
                    invocation.principal(), request.item(), request.ticket());
            ControlChanges.keep(context, ControlLog.Kind.PURGE, id,
                    invocation.principal(), Map.of("item",
                            request.item().toString(), "ticket",
                            request.ticket()));
            long until = request.waitFor() == null ? Long.MAX_VALUE
                    : System.nanoTime() + request.waitFor().toNanos();
            while (now.state().active() && System.nanoTime() < until)
            {
                Thread.sleep(Math.min(KeyCommands.LOOK.toMillis(), Math.max(1,
                        (until - System.nanoTime()) / 1_000_000)));
                now = requests.get(id).orElseThrow(() ->
                        new IllegalStateException("The purge is gone"));
            }
            invocation.audit().add(Attr.FILES, now.files());
            invocation.audit().add(Attr.ROWS, now.rows());
            if (now.state() == PurgeRequests.State.FAILED)
            {
                throw new IllegalStateException("The purge failed: "
                        + now.failure());
            }
            return now;
        }

        @Override
        public Map<String, Object> answer(PurgeRequests.Purge result)
        {
            Map<String, Object> answer = purgeAnswer(result);
            answer.put("delivered", "Copies delivered out of the pipeline"
                    + " are the receiver's to purge");
            return answer;
        }

        @Override
        public void print(PurgeRequests.Purge result, PrintStream out)
        {
            printPurge(result, out);
            if (result.state().active())
            {
                out.println("Still under way; purges shows how far it is");
            }
            out.println("Copies delivered out of the pipeline are the"
                    + " receiver's to purge");
        }
    }

    /**
     * {@code purges}: the stage's latest purges, and where they stand.
     *
     * @param <C> the stage's context
     */
    static final class Purges<C extends DataAdmin> implements
            CliOperation<C, Purges.Request, List<PurgeRequests.Purge>>,
            Answering<List<PurgeRequests.Purge>>
    {

        /** The most listed. */
        static final int MAX = 200;

        /**
         * What is asked.
         *
         * @param limit the most to list
         */
        record Request(int limit)
        {
        }

        /** Creates the command. */
        Purges()
        {
        }

        @Override
        public String name()
        {
            return "purges";
        }

        @Override
        public String usage()
        {
            return """
                    purges [--limit <n>]
                        The stage's latest item purges, newest first (20, or
                        up to 200): who asked, under which ticket, and how
                        far each is. Changes nothing.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("limit");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.DATA_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.QUERY;
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            Optional<String> given = arguments.option("limit");
            if (given.isEmpty())
            {
                return new Request(20);
            }
            try
            {
                int limit = Integer.parseInt(given.get());
                if (limit >= 1 && limit <= MAX)
                {
                    return new Request(limit);
                }
            }
            catch (NumberFormatException malformed)
            {
                // Said below.
            }
            throw new AdminArgs.UsageException("--limit must be 1 to " + MAX);
        }

        @Override
        public List<PurgeRequests.Purge> run(Request request, C context,
                Invocation invocation) throws Exception
        {
            PurgeRequests requests = context.purgeRequests(false);
            invocation.audit().add(Attr.CONTROL_SCOPE, requests.scope());
            return requests.recent(request.limit());
        }

        @Override
        public Map<String, Object> answer(List<PurgeRequests.Purge> result)
        {
            return Answers.object("purges", Answers.list(result,
                    DataCommands::purgeAnswer));
        }

        @Override
        public void print(List<PurgeRequests.Purge> result, PrintStream out)
        {
            if (result.isEmpty())
            {
                out.println("No purges");
            }
            result.forEach(each -> printPurge(each, out));
        }
    }

    /**
     * A table an erase dropped.
     *
     * @param name     its name
     * @param location where its files were
     * @param objects  the objects removed under it, after the drop
     * @param prefix   whether its location was removed as a prefix; if
     *                 not, its storage can't list, and only the files the
     *                 table knew of were removed
     */
    public record Dropped(String name, String location, long objects,
            boolean prefix)
    {
    }

    /**
     * An audit chain's last head, which the certificate names.
     *
     * @param chainId the chain
     * @param seq     its last entry
     * @param hash    that entry's hash, in hex
     */
    public record Head(UUID chainId, long seq, String hash)
    {
    }

    /**
     * What {@code erase} answers: the erasure, and its certificate.
     *
     * @param scope       the scope
     * @param id          the operation
     * @param claims      what the certificate says
     * @param certificate the certificate: a JWS, signed with the stage's
     *                    key
     */
    public record Erasure(String scope, String id, Map<String, Object> claims,
            String certificate)
    {
    }

    /**
     * {@code erase}: removes every table and row the stage keeps, at the
     * end of a contract, once it is paused and drained; then its pods
     * refuse to start. It needs a second person's approval and a ticket.
     * The result is a certificate, signed with the stage's key, naming
     * what was removed, the KEKs and signing key for the key custodian to
     * destroy, and the journal's last heads. The journal itself is kept:
     * export it first.
     *
     * @param <C> the stage's context
     */
    static final class Erase<C extends DataAdmin> implements
            CliOperation<C, Erase.Request, Erasure>, Answering<Erasure>
    {

        /**
         * What is asked.
         *
         * @param ticket the ticket it is asked under
         */
        record Request(String ticket)
        {
        }

        /** Creates the command. */
        Erase()
        {
        }

        @Override
        public String name()
        {
            return "erase";
        }

        @Override
        public String usage()
        {
            return """
                    erase --ticket <ref>
                        At the end of a contract: drops every table the
                        stage writes, with its files and its storage
                        prefixes, and deletes its database rows. Its pods
                        then refuse to start. Refused unless the stage is
                        paused and drained. Export the audit journal first:
                        it is kept. Answers a certificate signed with the
                        stage's key, naming what was removed and the KEKs
                        for the key custodian to destroy. Can't be undone.
                        Needs a second person's approval through the API,
                        and a ticket.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("ticket");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.DATA_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.LONG;
        }

        @Override
        public Approval approval(Request request)
        {
            return Approval.FOUR_EYES;
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request(ticket(arguments));
        }

        /**
         * Checks the stage is paused and drained, marks it erased, drops
         * its tables and deletes its rows, and signs the certificate. A run
         * taken over, or asked again, carries on with what is left.
         *
         * @throws AdminArgs.UsageException if it isn't paused and drained
         */
        @Override
        public Erasure run(Request request, C context, Invocation invocation)
                throws Exception
        {
            ControlState state = context.controlState(true);
            String scope = state.scope();
            invocation.audit().add(Attr.CONTROL_SCOPE, scope);
            invocation.audit().add(Attr.TICKET, request.ticket());
            refuseUnlessDrained(state, context);
            String id = operationId(invocation);
            Set<String> keks = keks(state, context);
            List<Head> heads = heads(context, scope);

            ControlState.Erased erased = state.erase(id,
                    invocation.principal());
            // Kept before the tables go, since the log is found by them.
            ControlChanges.keep(context, ControlLog.Kind.ERASE, id,
                    invocation.principal(), Map.of("ticket",
                            request.ticket()));
            List<Dropped> dropped = new ArrayList<>();
            for (DataAdmin.Erasable each : context.erasable())
            {
                drop(each).ifPresent(dropped::add);
            }
            Map<String, Long> rows = deleteRows(context);

            Map<String, String> audit = invocation.audit().fields()
                    .toStrings();
            KeyMaterial keys = context.signingKeys();
            Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("scope", scope);
            claims.put("operation", id);
            claims.put("erasure", erased.id());
            claims.put("requestedBy", invocation.principal());
            claims.put("approvedBy", audit.get(Attr.APPROVED_BY.key()));
            claims.put("ticket", request.ticket());
            claims.put("erasedAt", erased.erasedAt() == null ? null
                    : erased.erasedAt().toString());
            claims.put("at", Instant.now().toString());
            claims.put("iat", Instant.now().getEpochSecond());
            List<Object> tables = new ArrayList<>();
            long objects = 0;
            for (Dropped each : dropped)
            {
                tables.add(Answers.object("name", each.name(),
                        "location", each.location(),
                        "objects", each.objects(),
                        "prefixRemoved", each.prefix()));
                objects += each.objects();
            }
            claims.put("tables", tables);
            claims.put("rows", new LinkedHashMap<>(rows));
            claims.put("keks", new ArrayList<>(keks));
            claims.put("signingKid", keys.signerKid());
            List<Object> chains = new ArrayList<>();
            for (Head head : heads)
            {
                chains.add(Answers.object("chainId", head.chainId()
                        .toString(), "seq", head.seq(), "hash",
                        head.hash()));
            }
            claims.put("journalHeads", chains);
            claims.put("journal", "kept: audit records are kept six years"
                    + " and hold no PHI");
            claims.put("custodian", "destroy the KEKs and the signing key"
                    + " named here, in the KMS, so no copy left anywhere can"
                    + " be read");
            JWSObject jws = new JWSObject(new JWSHeader.Builder(
                    JWSAlgorithm.ES256).keyID(keys.signerKid())
                    .type(new JOSEObjectType(CERTIFICATE_TYPE)).build(),
                    new Payload(JSONObjectUtils.toJSONString(claims)));
            jws.sign(keys.signer());
            invocation.audit().add(Attr.TABLES, dropped.size());
            invocation.audit().add(Attr.OBJECTS, objects);
            invocation.audit().add(Attr.ROWS, rows.values().stream()
                    .mapToLong(Long::longValue).sum());
            return new Erasure(scope, id, claims, jws.serialize());
        }

        @Override
        public Map<String, Object> answer(Erasure result)
        {
            return Answers.object("scope", result.scope(),
                    "id", result.id(), "erasure", result.claims(),
                    "certificate", result.certificate());
        }

        @Override
        public void print(Erasure result, PrintStream out)
        {
            out.println("Erased " + result.scope() + ": "
                    + ((List<?>) result.claims().get("tables")).size()
                    + " table(s); its pods now refuse to start");
            out.println("KEKs for the key custodian to destroy: "
                    + result.claims().get("keks"));
            out.println("Certificate:");
            out.println(result.certificate());
        }
    }

    /**
     * Refuses an erase unless the stage is paused, and, if it drains,
     * paused long enough for every pod to have read it, with no live
     * claim.
     *
     * @param state   the scope's state
     * @param context the stage's context
     * @throws AdminArgs.UsageException if it isn't
     * @throws Exception                if the claims can't be read
     */
    static void refuseUnlessDrained(ControlState state, DataAdmin context)
            throws Exception
    {
        Optional<ControlState.Paused> paused = state.paused();
        if (paused.isEmpty())
        {
            throw new AdminArgs.UsageException(state.scope() + " isn't"
                    + " paused: pause and drain it first");
        }
        if (!context.drains())
        {
            return;
        }
        Instant at = paused.get().pausedAt();
        if (at == null || Instant.now().isBefore(at.plus(DrainCommand.SETTLE)))
        {
            throw new AdminArgs.UsageException(state.scope() + " was paused"
                    + " too lately to be drained: try again in "
                    + DrainCommand.SETTLE.toSeconds() + " seconds");
        }
        long claims = context.liveClaims();
        if (claims > 0)
        {
            throw new AdminArgs.UsageException(state.scope() + " isn't"
                    + " drained: " + claims + " live claim(s)");
        }
    }

    /**
     * Every KEK the stage stores under, or was set to: for the key
     * custodian to destroy.
     *
     * @param state   the scope's state
     * @param context the stage's context
     * @return their references, sorted
     * @throws Exception if the tables or database can't be read
     */
    static Set<String> keks(ControlState state, DataAdmin context)
            throws Exception
    {
        List<String> found = new ArrayList<>();
        found.add(context.settingKek());
        state.kek().ifPresent(kek -> found.add(kek.kek()));
        state.rekeying().ifPresent(rekey ->
        {
            found.add(rekey.from());
            found.add(rekey.to());
        });
        for (Table table : context.keyTables())
        {
            table.refresh();
            found.addAll(Rekey.byKek(table).keySet());
            IdKeys.kek(table).ifPresent(found::add);
        }
        for (WrappedRows each : context.wrappedRows())
        {
            found.addAll(each.byKek(context.keyDatabase(), context.stage())
                    .keySet());
        }
        Set<String> keks = new TreeSet<>();
        found.stream().filter(each -> each != null && !each.isBlank())
                .forEach(keks::add);
        return keks;
    }

    /**
     * The last heads of the scope's audit chains.
     *
     * @param context the stage's context
     * @param scope   the scope, which its chains are of
     * @return them, by chain
     * @throws IllegalStateException if the journal can't be read
     */
    static List<Head> heads(DataAdmin context, String scope)
    {
        String sql = "SELECT chain_id, last_seq, last_hash FROM "
                + context.auditTables().chains()
                + " WHERE source = ? ORDER BY chain_id";
        List<Head> heads = new ArrayList<>();
        try (Connection connection = context.keyDatabase().getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(60);
            select.setString(1, scope);
            try (ResultSet rows = select.executeQuery())
            {
                while (rows.next())
                {
                    heads.add(new Head(rows.getObject(1, UUID.class),
                            rows.getLong(2),
                            HexFormat.of().formatHex(rows.getBytes(3))));
                }
            }
        }
        catch (SQLException failure)
        {
            if ("42P01".equals(failure.getSQLState()))
            {
                return heads;
            }
            throw new IllegalStateException("Audit journal unavailable",
                    failure);
        }
        return heads;
    }

    /**
     * Drops a table with its data files, then removes what is left under
     * its location, if its storage can list.
     *
     * @param table the table
     * @return what was dropped; empty if it wasn't there
     */
    static Optional<Dropped> drop(DataAdmin.Erasable table)
    {
        if (!table.catalog().tableExists(table.id()))
        {
            return Optional.empty();
        }
        Table loaded = table.catalog().loadTable(table.id());
        String location = loaded.location();
        FileIO io = loaded.io();
        table.catalog().dropTable(table.id(), true);
        long objects = 0;
        boolean prefix = io instanceof SupportsPrefixOperations;
        if (io instanceof SupportsPrefixOperations listing)
        {
            String under = location.endsWith("/") ? location
                    : location + "/";
            for (FileInfo ignored : listing.listPrefix(under))
            {
                objects++;
            }
            listing.deletePrefix(under);
        }
        return Optional.of(new Dropped(table.id().toString(), location,
                objects, prefix));
    }

    /**
     * Deletes the stage's rows from the database tables it keeps with
     * wrapped keys: those stages share by its stage column, its own
     * whole.
     *
     * @param context the stage's context
     * @return rows deleted, by table
     * @throws IllegalStateException if the database can't be reached
     */
    static Map<String, Long> deleteRows(DataAdmin context)
    {
        Map<String, Long> deleted = new LinkedHashMap<>();
        DataSource database = context.dataToChange();
        for (WrappedRows each : context.wrappedRows())
        {
            String sql = "DELETE FROM " + each.table() + (each.stage() == null
                    ? "" : " WHERE " + each.stage() + " = ?");
            try (Connection connection = database.getConnection();
                    PreparedStatement delete = connection.prepareStatement(
                            sql))
            {
                delete.setQueryTimeout(600);
                if (each.stage() != null)
                {
                    delete.setString(1, context.stage());
                }
                deleted.put(each.table(), (long) delete.executeUpdate());
            }
            catch (SQLException failure)
            {
                if ("42P01".equals(failure.getSQLState()))
                {
                    continue;
                }
                throw new IllegalStateException("Could not delete the"
                        + " stage's rows", failure);
            }
        }
        return deleted;
    }
}
