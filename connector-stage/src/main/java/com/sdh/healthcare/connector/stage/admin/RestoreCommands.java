/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.crypto.WrappedKey;
import com.sdh.healthcare.connector.core.maintenance.TableRestore;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.ControlLog;
import com.sdh.healthcare.connector.stage.ControlState;
import com.sdh.healthcare.connector.stage.PurgeRequests;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.iceberg.Table;

/**
 * A restore's commands (DESIGN.md §18.7, HIPAA.md P7), run on each stage
 * after the pipeline's database was restored to an earlier point and
 * before any of its pods starts: {@code restore-check}, which looks and
 * changes nothing; {@code restore-repair}, which makes the stage whole
 * again; and {@code restore-release}, which lets the stage's maintenance
 * remove the files a repair kept, once the pipeline has caught up.
 * <p>
 * A repair, for each table the stage writes: brings back from the
 * store's versions every file the restored catalog references that was
 * deleted since; for the landing node, points the catalog at its tables'
 * newest metadata instead, so no landed item is lost, and raises the
 * leases' epochs to theirs; and keeps the files nothing references. Then
 * it makes again the purges and erasures the store's control log has and
 * the database doesn't, and reports the rotations, re-keys and retention
 * changes to make again. It tries each KEK the stage stores under.
 * <p>
 * Each answer names tables, files, operations and KEKs, and counts: never
 * a record or a document's value.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class RestoreCommands
{

    /**
     * In place of a KEK: those the tables record, when their manifests
     * can't all be read, as until a repair brings them back.
     */
    static final String TABLES_KEKS = "(the tables' own)";

    /** The most missing files named in an answer, of each table. */
    static final int NAMED = 20;

    /** A table that answers the lease's columns. */
    private static final Pattern TABLE =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,62}"
                    + "(\\.[A-Za-z_][A-Za-z0-9_]{0,62})?");

    /** Not created: holds the commands. */
    private RestoreCommands()
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
        return List.of(new Check<>(false), new Check<>(true),
                new Release<>());
    }

    /**
     * What became of a table.
     *
     * @param table      its name
     * @param state      {@code checked}, {@code forward} (pointed at its
     *                   newest metadata), {@code rivals} (newest metadata
     *                   to choose between), {@code absent} or
     *                   {@code unloadable}
     * @param referenced the files its metadata references
     * @param recovered  those brought back from the store's versions
     * @param missing    the first of those missing, by location
     * @param missingAll how many are missing
     * @param complete   false if a manifest was missing, so not every file
     *                   was seen
     * @param orphans    the files nothing references
     * @param orphanSize their bytes
     * @param listed     false if the store can't list, so orphans weren't
     *                   counted
     * @param forwardTo  the metadata the catalog was, or would be, pointed
     *                   at
     * @param rivals     the newest metadata files, when more than one
     * @param epoch      the lease epoch the newest metadata records
     */
    public record TableState(String table, String state, long referenced,
            int recovered, List<String> missing, int missingAll,
            boolean complete, long orphans, long orphanSize, boolean listed,
            Optional<String> forwardTo, List<String> rivals, long epoch)
    {

        /**
         * Copies the lists.
         */
        public TableState
        {
            missing = List.copyOf(missing);
            rivals = List.copyOf(rivals);
        }

        /**
         * Whether the table is whole.
         *
         * @return true if nothing is missing and no choice is left
         */
        public boolean whole()
        {
            return switch (state)
            {
                case "checked", "forward", "absent" ->
                        missingAll == 0 && complete;
                default -> false;
            };
        }

        /**
         * A table the stage hasn't made, or that can't be loaded.
         *
         * @param table its name
         * @param state {@code absent} or {@code unloadable}
         * @return it
         */
        static TableState of(String table, String state)
        {
            return new TableState(table, state, 0, 0, List.of(), 0, true, 0,
                    0, true, Optional.empty(), List.of(), 0);
        }
    }

    /**
     * What became of a control change kept in the store.
     *
     * @param id    its operation
     * @param kind  its kind
     * @param at    when it was made
     * @param state {@code in-database} (the restore kept it),
     *              {@code replayed} (made again), {@code to-replay} (to
     *              make again: a repair does), {@code to-redo} (for the
     *              operator to make again), or {@code superseded} (a later
     *              one of its kind decides)
     */
    public record Change(String id, String kind, Instant at, String state)
    {

        /**
         * Whether it still needs doing.
         *
         * @return true if it does
         */
        public boolean open()
        {
            return state.equals("to-replay") || state.equals("to-redo");
        }
    }

    /**
     * Whether a KEK could be used.
     *
     * @param kek     its reference
     * @param usable  whether a key wrapped under it unwrapped again
     * @param failure the failure's kind if it didn't, never its text
     */
    public record Kek(String kek, boolean usable, String failure)
    {
    }

    /**
     * What a check or repair found.
     *
     * @param scope      the scope
     * @param repaired   whether it repaired
     * @param tables     its tables
     * @param changes    the control changes kept in the store
     * @param logRead    false if they couldn't be read
     * @param keks       the KEKs tried
     * @param paused     whether the stage is paused, as restored
     * @param epochFloor the epoch the leases were raised to, 0 if none
     */
    public record Report(String scope, boolean repaired,
            List<TableState> tables, List<Change> changes, boolean logRead,
            List<Kek> keks, boolean paused, long epochFloor)
    {

        /**
         * Copies the lists.
         */
        public Report
        {
            tables = List.copyOf(tables);
            changes = List.copyOf(changes);
            keks = List.copyOf(keks);
        }

        /**
         * Whether the stage may start: every table whole, every change
         * made, every KEK usable.
         *
         * @return true if it may
         */
        public boolean whole()
        {
            return logRead
                    && tables.stream().allMatch(TableState::whole)
                    && changes.stream().noneMatch(Change::open)
                    && keks.stream().allMatch(Kek::usable);
        }
    }

    /**
     * {@code restore-check} and {@code restore-repair}.
     *
     * @param <C> the stage's context
     */
    static final class Check<C extends DataAdmin> implements
            CliOperation<C, Check.Request, Report>, Answering<Report>
    {

        /**
         * What is asked.
         *
         * @param ticket the ticket a repair is asked under; null for a check
         */
        record Request(String ticket)
        {
        }

        /** Whether it repairs. */
        private final boolean repair;

        /**
         * Creates the command.
         *
         * @param repair whether it repairs
         */
        Check(boolean repair)
        {
            this.repair = repair;
        }

        @Override
        public String name()
        {
            return repair ? "restore-repair" : "restore-check";
        }

        @Override
        public String usage()
        {
            return repair ? """
                    restore-repair --ticket <ref>
                        After the pipeline's database is restored to an
                        earlier point, before any pod of the stage starts:
                        brings back from the store's versions each file the
                        restored catalog needs that was deleted since; for
                        the landing node, points its tables at their newest
                        metadata, so no landed item is lost, and raises the
                        leases' epochs to match; keeps every file nothing
                        references until restore-release; makes again the
                        purges and erasures the store's control log holds
                        and the database doesn't; and tries each KEK.
                        Refused while a lease of the stage is live. Needs a
                        second person's approval through the API, and a
                        ticket."""
                    : """
                    restore-check
                        What restore-repair would find and do, changing
                        nothing: each table's missing files, newer
                        metadata and orphans, the control changes the
                        database lacks, and the KEKs that can't be used.
                        Refused while a lease of the stage is live.""";
        }

        @Override
        public Set<String> options()
        {
            return repair ? Set.of("ticket") : Set.of();
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
            return repair ? Kind.LONG : Kind.QUERY;
        }

        @Override
        public Approval approval(Request request)
        {
            return repair ? Approval.FOUR_EYES : Approval.NONE;
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request(repair ? DataCommands.ticket(arguments) : null);
        }

        @Override
        public Report run(Request request, C context, Invocation invocation)
                throws Exception
        {
            ControlState state = context.controlState(repair);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            if (request.ticket() != null)
            {
                invocation.audit().add(Attr.TICKET, request.ticket());
            }
            refuseIfLive(context);
            Report report = check(context, state, repair);
            invocation.audit().add(Attr.TABLES, report.tables().size());
            invocation.audit().add(Attr.FILES, report.tables().stream()
                    .mapToLong(TableState::recovered).sum());
            invocation.audit().add(Attr.OBJECTS, report.tables().stream()
                    .mapToLong(TableState::orphans).sum());
            invocation.audit().add(Attr.RESULT, report.whole() ? "whole"
                    : "not_whole");
            return report;
        }

        @Override
        public Map<String, Object> answer(Report result)
        {
            return RestoreCommands.answer(result);
        }

        @Override
        public void print(Report result, PrintStream out)
        {
            RestoreCommands.print(result, out);
        }
    }

    /**
     * {@code restore-release}: lets each table's maintenance remove the
     * files nothing references again, once the pipeline has caught up
     * after a restore.
     *
     * @param <C> the stage's context
     */
    static final class Release<C extends DataAdmin> implements
            CliOperation<C, Release.Request, List<String>>,
            Answering<List<String>>
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
        Release()
        {
        }

        @Override
        public String name()
        {
            return "restore-release";
        }

        @Override
        public String usage()
        {
            return """
                    restore-release --ticket <ref>
                        Once the pipeline has caught up after a restore:
                        lets the stage's maintenance remove the files that
                        restore-repair kept, which nothing references, at
                        its next run. Needs a second person's approval
                        through the API, and a ticket.""";
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
            return Kind.ACTION;
        }

        @Override
        public Approval approval(Request request)
        {
            return Approval.FOUR_EYES;
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request(DataCommands.ticket(arguments));
        }

        @Override
        public List<String> run(Request request, C context,
                Invocation invocation) throws Exception
        {
            invocation.audit().add(Attr.CONTROL_SCOPE,
                    context.controlState(false).scope());
            invocation.audit().add(Attr.TICKET, request.ticket());
            List<String> released = new ArrayList<>();
            for (DataAdmin.Erasable each : context.erasable())
            {
                if (each.catalog().tableExists(each.id()))
                {
                    Table table = each.catalog().loadTable(each.id());
                    if (TableRestore.release(table))
                    {
                        released.add(each.id().toString());
                    }
                }
            }
            invocation.audit().add(Attr.TABLES, released.size());
            return released;
        }

        @Override
        public Map<String, Object> answer(List<String> result)
        {
            return Answers.object("released", new ArrayList<>(result));
        }

        @Override
        public void print(List<String> result, PrintStream out)
        {
            out.println(result.isEmpty() ? "No table was keeping its orphans"
                    : "Released: " + String.join(", ", result));
        }
    }

    /**
     * Refuses while a lease of the stage is live: a pod is running, and
     * would write while the tables are checked.
     *
     * @param context the stage's context
     * @throws AdminArgs.UsageException if one is
     * @throws SQLException             if the leases can't be read
     */
    static void refuseIfLive(DataAdmin context) throws SQLException
    {
        Optional<String> table = context.leaseTable();
        if (table.isEmpty())
        {
            return;
        }
        String sql = "SELECT count(*) FROM " + checked(table.get())
                + " WHERE expires_at > clock_timestamp()";
        long live = 0;
        try (Connection connection = context.keyDatabase().getConnection();
                PreparedStatement select = connection.prepareStatement(sql))
        {
            select.setQueryTimeout(60);
            try (ResultSet rows = select.executeQuery())
            {
                live = rows.next() ? rows.getLong(1) : 0;
            }
        }
        catch (SQLException failure)
        {
            if (!"42P01".equals(failure.getSQLState()))
            {
                throw failure;
            }
        }
        if (live > 0)
        {
            throw new AdminArgs.UsageException(live + " lease(s) of the"
                    + " stage are live: stop its pods (they release on the"
                    + " way out, or expire) and ask again");
        }
    }

    /**
     * Checks, and repairs if asked, the stage after a restore.
     *
     * @param context the stage's context
     * @param state   its control state
     * @param repair  whether to repair
     * @return what was found
     * @throws Exception if the tables, store or database can't be reached
     */
    static Report check(DataAdmin context, ControlState state,
            boolean repair) throws Exception
    {
        Instant now = Instant.now();
        List<TableState> tables = new ArrayList<>();
        long epoch = 0;
        for (DataAdmin.Erasable each : context.erasable())
        {
            TableState table = table(context, each, repair, now);
            tables.add(table);
            epoch = Math.max(epoch, table.epoch());
        }
        long floor = 0;
        if (repair && context.bringsForward() && epoch > 0)
        {
            floor = raiseEpochs(context, epoch);
        }

        List<Change> changes = new ArrayList<>();
        boolean read = true;
        try
        {
            Optional<ControlLog> log = context.controlLog();
            if (log.isPresent())
            {
                changes = changes(context, state, log.get().entries(),
                        repair);
            }
        }
        catch (IllegalStateException unreadable)
        {
            read = false;
        }
        return new Report(state.scope(), repair, tables, changes, read,
                keks(context, state), state.paused().isPresent(), floor);
    }

    /**
     * Checks, and repairs if asked, one table.
     *
     * @param context the stage's context
     * @param each    the table
     * @param repair  whether to repair
     * @param now     the time of the check
     * @return what became of it
     */
    static TableState table(DataAdmin context, DataAdmin.Erasable each,
            boolean repair, Instant now)
    {
        String name = each.id().toString();
        if (!each.catalog().tableExists(each.id()))
        {
            return TableState.of(name, "absent");
        }
        Table table;
        try
        {
            table = each.catalog().loadTable(each.id());
        }
        catch (RuntimeException unloadable)
        {
            // Its current metadata file is gone: bring that back by its
            // location from the catalog's row, with the store's tools.
            return TableState.of(name, "unloadable");
        }
        Optional<String> forwardTo = Optional.empty();
        List<String> rivals = List.of();
        long epoch = 0;
        String state = "checked";
        if (context.bringsForward())
        {
            TableRestore.Forward forward = TableRestore.newest(table);
            rivals = forward.rivals();
            forwardTo = forward.to();
            epoch = forward.epoch();
            if (!rivals.isEmpty())
            {
                state = "rivals";
            }
            else if (forwardTo.isPresent())
            {
                state = "forward";
                if (repair)
                {
                    table = TableRestore.pointAt(each.catalog(), each.id(),
                            forwardTo.get());
                }
            }
        }
        TableRestore.Walk walk = TableRestore.walk(table, repair);
        if (repair && walk.whole() && !state.equals("rivals"))
        {
            TableRestore.keepOrphans(table, now);
        }
        return new TableState(name, state, walk.referenced(),
                walk.recovered().size(),
                walk.missing().subList(0, Math.min(NAMED,
                        walk.missing().size())),
                walk.missing().size(), walk.complete(),
                walk.orphans().count(), walk.orphans().bytes(),
                walk.orphans().listed(), forwardTo, rivals, epoch);
    }

    /**
     * Raises the epoch of every lease in the stage's lease table that is
     * below the tables' own, so the next holder's commits aren't fenced
     * out by metadata newer than the restored leases. Epochs only grow, so
     * no holder is wronged.
     *
     * @param context the stage's context
     * @param epoch   the tables' highest epoch
     * @return the epoch the leases now have at least; 0 if there is no
     *         lease table
     * @throws SQLException if it can't be written
     */
    static long raiseEpochs(DataAdmin context, long epoch) throws SQLException
    {
        Optional<String> table = context.leaseTable();
        if (table.isEmpty())
        {
            return 0;
        }
        String sql = "UPDATE " + checked(table.get())
                + " SET epoch = ? WHERE epoch < ?";
        try (Connection connection = context.dataToChange().getConnection();
                PreparedStatement update = connection.prepareStatement(sql))
        {
            update.setQueryTimeout(60);
            update.setLong(1, epoch);
            update.setLong(2, epoch);
            update.executeUpdate();
        }
        return epoch;
    }

    /**
     * What became of each control change kept in the store; a repair
     * makes the purges and erasures again. Of the rotations, re-keys and
     * retention changes, only the latest of each kind decides.
     *
     * @param context the stage's context
     * @param state   its control state
     * @param entries the changes kept, oldest first
     * @param repair  whether to make them again
     * @return each one's state
     * @throws Exception if the database can't be read or written
     */
    static List<Change> changes(DataAdmin context, ControlState state,
            List<ControlLog.Entry> entries, boolean repair) throws Exception
    {
        Map<ControlLog.Kind, ControlLog.Entry> latest =
                new EnumMap<>(ControlLog.Kind.class);
        entries.forEach(entry -> latest.put(entry.kind(), entry));
        PurgeRequests purges = null;
        List<Change> changes = new ArrayList<>();
        for (ControlLog.Entry entry : entries)
        {
            String outcome;
            switch (entry.kind())
            {
                case PURGE ->
                {
                    if (purges == null)
                    {
                        purges = context.purgeRequests(repair);
                    }
                    if (purges.get(entry.id()).isPresent())
                    {
                        outcome = "in-database";
                    }
                    else if (repair)
                    {
                        purges.request(entry.id(), entry.by(),
                                UUID.fromString(entry.field("item")),
                                entry.field("ticket"));
                        outcome = "replayed";
                    }
                    else
                    {
                        outcome = "to-replay";
                    }
                }
                case ERASE ->
                {
                    Optional<ControlState.Erased> erased = state.erased();
                    if (erased.isPresent() && erased.get().id().equals(
                            entry.id()))
                    {
                        outcome = "in-database";
                    }
                    else if (repair)
                    {
                        // Marked, so the pods refuse to start; erase is
                        // asked again to drop the tables.
                        state.erase(entry.id(), entry.by());
                        outcome = "replayed";
                    }
                    else
                    {
                        outcome = "to-replay";
                    }
                }
                default -> outcome = latest.get(entry.kind()) != entry
                        ? "superseded"
                        : inDatabase(state, entry) ? "in-database"
                                : "to-redo";
            }
            changes.add(new Change(entry.id(), entry.kind().wire(),
                    entry.at(), outcome));
        }
        return changes;
    }

    /**
     * Whether the restored control state has a rotation, re-key or
     * retention change.
     *
     * @param state the control state
     * @param entry the change
     * @return true if it does
     */
    static boolean inDatabase(ControlState state, ControlLog.Entry entry)
    {
        return switch (entry.kind())
        {
            case ROTATE -> state.kek().map(kek -> kek.kek().equals(
                    entry.field("to"))).orElse(false);
            case REKEY -> state.rekeying().map(rekey -> rekey.id().equals(
                    entry.id())).orElse(false);
            case RETENTION -> state.retention().map(retention -> retention
                    .wire().equals(entry.field("retention"))).orElse(false);
            default -> true;
        };
    }

    /**
     * Tries each KEK the stage stores under: a fresh key wrapped under it
     * and unwrapped again.
     *
     * @param context the stage's context
     * @param state   its control state
     * @return each one's outcome
     * @throws Exception if the KEKs can't be listed
     */
    static List<Kek> keks(DataAdmin context, ControlState state)
            throws Exception
    {
        KeyProvider provider = context.keyProvider();
        SecureRandom random = new SecureRandom();
        List<Kek> keks = new ArrayList<>();
        Set<String> found;
        try
        {
            found = DataCommands.keks(state, context);
        }
        catch (RuntimeException unreadable)
        {
            // A table's manifests may be missing until the repair brings
            // them back: try the KEKs the settings and state name.
            Set<String> named = new TreeSet<>();
            named.add(context.settingKek());
            state.kek().ifPresent(kek -> named.add(kek.kek()));
            state.rekeying().ifPresent(rekey ->
            {
                named.add(rekey.from());
                named.add(rekey.to());
            });
            named.removeIf(each -> each == null || each.isBlank());
            found = named;
            keks.add(new Kek(TABLES_KEKS, false,
                    unreadable.getClass().getSimpleName()));
        }
        for (String kek : found)
        {
            byte[] key = new byte[32];
            random.nextBytes(key);
            try
            {
                WrappedKey wrapped = provider.wrap(kek, key);
                boolean same = Arrays.equals(key, provider.unwrap(wrapped));
                keks.add(new Kek(kek, same, same ? null : "Mismatch"));
            }
            catch (RuntimeException failure)
            {
                keks.add(new Kek(kek, false,
                        failure.getClass().getSimpleName()));
            }
        }
        return keks;
    }

    /**
     * A lease table's name, checked to be one.
     *
     * @param table the name
     * @return it
     * @throws IllegalArgumentException if it isn't one
     */
    static String checked(String table)
    {
        if (!TABLE.matcher(table).matches())
        {
            throw new IllegalArgumentException("Not a table's name");
        }
        return table;
    }

    /**
     * A report, as answered.
     *
     * @param report the report
     * @return it
     */
    static Map<String, Object> answer(Report report)
    {
        Map<String, Object> answer = new LinkedHashMap<>();
        answer.put("scope", report.scope());
        answer.put("repaired", report.repaired());
        answer.put("whole", report.whole());
        answer.put("paused", report.paused());
        answer.put("epochFloor", report.epochFloor());
        answer.put("tables", Answers.list(report.tables(), table ->
        {
            Map<String, Object> each = new LinkedHashMap<>();
            each.put("table", table.table());
            each.put("state", table.state());
            each.put("referenced", table.referenced());
            each.put("recovered", table.recovered());
            each.put("missing", table.missingAll());
            each.put("missingFiles", new ArrayList<>(table.missing()));
            each.put("complete", table.complete());
            each.put("orphans", table.orphans());
            each.put("orphanBytes", table.orphanSize());
            each.put("orphansCounted", table.listed());
            each.put("forwardTo", table.forwardTo().orElse(null));
            each.put("rivals", new ArrayList<>(table.rivals()));
            return each;
        }));
        answer.put("controlLogRead", report.logRead());
        answer.put("changes", Answers.list(report.changes(), change ->
                Answers.object("id", change.id(), "kind", change.kind(),
                        "at", change.at().toString(), "state",
                        change.state())));
        answer.put("keks", Answers.list(report.keks(), kek ->
        {
            Map<String, Object> each = new LinkedHashMap<>();
            each.put("kek", kek.kek());
            each.put("usable", kek.usable());
            each.put("failure", kek.failure());
            return each;
        }));
        return answer;
    }

    /**
     * Prints a report.
     *
     * @param report the report
     * @param out    where
     */
    static void print(Report report, PrintStream out)
    {
        out.println(report.scope() + (report.repaired() ? ": repaired"
                : ": checked") + (report.whole() ? "; whole, may start"
                        : "; NOT whole"));
        for (TableState table : report.tables())
        {
            out.println("  " + table.table() + ": " + table.state()
                    + ", " + table.referenced() + " files referenced, "
                    + table.recovered() + " brought back, "
                    + table.missingAll() + " missing"
                    + (table.complete() ? "" : " (not all seen)")
                    + (table.listed() ? ", " + table.orphans()
                            + " orphan(s) kept" : ", orphans not counted"));
            table.forwardTo().ifPresent(to -> out.println(
                    "    forward to " + to));
            table.rivals().forEach(rival -> out.println("    rival: "
                    + rival));
            table.missing().forEach(missing -> out.println("    missing: "
                    + missing));
        }
        if (!report.logRead())
        {
            out.println("  The control log in the store can't be read");
        }
        for (Change change : report.changes())
        {
            if (!change.state().equals("in-database"))
            {
                out.println("  " + change.kind() + " " + change.id() + " of "
                        + change.at() + ": " + change.state());
            }
        }
        for (Kek kek : report.keks())
        {
            if (kek.kek().equals(TABLES_KEKS))
            {
                out.println("  The KEKs the tables record can't be read ("
                        + kek.failure() + "): repair, then check again");
            }
            else if (!kek.usable())
            {
                out.println("  KEK " + kek.kek() + " can't be used ("
                        + kek.failure() + "): ask the key custodian to"
                        + " enable it");
            }
        }
        if (report.epochFloor() > 0)
        {
            out.println("  Leases raised to epoch " + report.epochFloor());
        }
        out.println("  " + (report.paused() ? "Paused" : "Not paused")
                + ", as restored: pause or resume it as wanted");
    }
}
