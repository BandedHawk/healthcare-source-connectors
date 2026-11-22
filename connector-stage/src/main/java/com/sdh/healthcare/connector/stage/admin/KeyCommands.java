/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.WrappedRows;
import com.sdh.healthcare.connector.core.maintenance.Rekey;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.runtime.config.KeyMaterial;
import com.sdh.healthcare.connector.stage.ControlState;
import java.io.PrintStream;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.iceberg.Table;

/**
 * Every stage's key commands (CONTROL-API-DESIGN.md §11, HIPAA.md P3):
 * {@code keys}, what the stage stores wrapped with which KEK;
 * {@code rotate}, the KEK its new data is wrapped with; and {@code rekey},
 * moving what it stores from one KEK to another, with a second person's
 * approval. None of them disables a KEK: that is the key custodian's act,
 * in the KMS, once {@code keys} shows nothing left under it.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class KeyCommands
{

    /** A KEK reference: a KMS key's ARN or alias, or a local key's name. */
    static final Pattern KEK = Pattern.compile("[A-Za-z0-9:/_.+=@-]{1,256}");

    /** How often a re-key's run looks at where it stands. */
    static final Duration LOOK = Duration.ofSeconds(30);

    /** Not created: holds the commands. */
    private KeyCommands()
    {
    }

    /**
     * The commands.
     *
     * @param <C> the stage's context
     * @return them
     */
    public static <C extends KeyAdmin> List<AdminCommand<C>> all()
    {
        return List.of(new Keys<>(), new Rotate<>(), new Rekeys<>());
    }

    /**
     * Reads a KEK reference.
     *
     * @param name  the option's name
     * @param value its value
     * @return the reference
     * @throws AdminArgs.UsageException if it isn't one
     */
    static String kek(String name, String value)
    {
        if (!KEK.matcher(value).matches())
        {
            throw new AdminArgs.UsageException("--" + name + " must be a KEK"
                    + " reference: 1 to 256 letters, digits or : / _ . + = @"
                    + " -");
        }
        return value;
    }

    /**
     * The KEK the scope's new data is wrapped with now.
     *
     * @param state   the scope's state
     * @param context the stage's context
     * @return its reference
     */
    static String active(ControlState state, KeyAdmin context)
    {
        return state.kek().map(ControlState.Kek::kek)
                .orElse(context.settingKek());
    }

    /**
     * A re-key as the API answers it.
     *
     * @param rekeying the re-key; empty if there has been none
     * @return the answer; null if there has been none
     */
    static Map<String, Object> rekeyAnswer(
            Optional<ControlState.Rekeying> rekeying)
    {
        if (rekeying.isEmpty())
        {
            return null;
        }
        ControlState.Rekeying rekey = rekeying.get();
        return Answers.object("id", rekey.id(), "from", rekey.from(),
                "to", rekey.to(), "state", rekey.state().wire(),
                "requestedBy", rekey.requestedBy(),
                "requestedAt", rekey.requestedAt(), "files", rekey.files(),
                "rows", rekey.rows(), "failure", rekey.failure(),
                "updatedAt", rekey.updatedAt());
    }

    /**
     * Prints a re-key.
     *
     * @param rekey the re-key
     * @param out   where to
     */
    static void printRekey(ControlState.Rekeying rekey, PrintStream out)
    {
        out.println("Re-key " + rekey.id() + " from " + rekey.from() + " to "
                + rekey.to() + ": " + rekey.state().wire() + ", "
                + rekey.files() + " file(s), " + rekey.rows() + " row(s)"
                + (rekey.failure() == null ? "" : ", " + rekey.failure())
                + "; asked by " + rekey.requestedBy() + " at "
                + rekey.requestedAt());
    }

    /**
     * What the scope stores under each KEK.
     *
     * @param scope    the scope
     * @param active   the KEK its new data is wrapped with
     * @param rotated  its rotation; empty if it uses its setting's KEK
     * @param setting  its setting's KEK
     * @param tables   rows of each table, by KEK
     * @param idKeys   each table's ID key's KEK, if it has one
     * @param rows     rows of each database table, by KEK
     * @param rekeying its re-key under way or last; empty if none
     */
    public record KeyState(String scope, String active,
            Optional<ControlState.Kek> rotated, String setting,
            Map<String, Map<String, Long>> tables, Map<String, String> idKeys,
            Map<String, Map<String, Long>> rows,
            Optional<ControlState.Rekeying> rekeying)
    {
    }

    /**
     * {@code keys}: the KEK in use, and how many rows each KEK wraps.
     *
     * @param <C> the stage's context
     */
    static final class Keys<C extends KeyAdmin> implements CliOperation<C,
            Keys.Request, KeyState>, Answering<KeyState>
    {

        /** What is asked: nothing but the keys. */
        record Request()
        {
        }

        /** Creates the command. */
        Keys()
        {
        }

        @Override
        public String name()
        {
            return "keys";
        }

        @Override
        public String usage()
        {
            return """
                    keys
                        The KEK the stage's new data is wrapped with, and
                        whether it was rotated to; how many rows of each
                        table, and of each database table, each KEK wraps;
                        and the last re-key. Reads every table's KEK column,
                        so it takes a while on a large stage. Changes
                        nothing.""";
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
            return Permission.KEYS_ADMIN;
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
        public KeyState run(Request request, C context,
                Invocation invocation) throws Exception
        {
            ControlState state = context.controlState(false);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            Optional<ControlState.Kek> rotated = state.kek();
            String active = rotated.map(ControlState.Kek::kek)
                    .orElse(context.settingKek());
            invocation.audit().add(Attr.KEK, active);
            Map<String, Map<String, Long>> tables = new LinkedHashMap<>();
            Map<String, String> idKeys = new LinkedHashMap<>();
            for (Table table : context.keyTables())
            {
                table.refresh();
                tables.put(table.name(), Rekey.byKek(table));
                IdKeys.kek(table).ifPresent(kek ->
                        idKeys.put(table.name(), kek));
            }
            Map<String, Map<String, Long>> rows = new LinkedHashMap<>();
            for (WrappedRows each : context.wrappedRows())
            {
                rows.put(each.table(), each.byKek(context.keyDatabase(),
                        context.stage()));
            }
            return new KeyState(state.scope(), active, rotated,
                    context.settingKek(), tables, idKeys, rows,
                    state.rekeying());
        }

        @Override
        public Map<String, Object> answer(KeyState result)
        {
            return Answers.object("scope", result.scope(),
                    "active", result.active(),
                    "rotated", result.rotated().isPresent(),
                    "rotatedBy", result.rotated().map(ControlState.Kek::setBy)
                            .orElse(null),
                    "rotatedAt", result.rotated().map(ControlState.Kek::setAt)
                            .orElse(null),
                    "setting", result.setting(),
                    "tables", result.tables(), "idKeys", result.idKeys(),
                    "rows", result.rows(),
                    "rekey", rekeyAnswer(result.rekeying()));
        }

        @Override
        public void print(KeyState result, PrintStream out)
        {
            out.println(result.scope() + " wraps new data with "
                    + result.active() + result.rotated().map(kek ->
                            ", rotated to by " + kek.setBy() + " at "
                                    + kek.setAt())
                            .orElse(", its setting's"));
            print("Table", result.tables(), out);
            result.idKeys().forEach((table, kek) -> out.println("ID key of "
                    + table + ": " + kek));
            print("Database table", result.rows(), out);
            result.rekeying().ifPresent(rekey -> printRekey(rekey, out));
        }

        /**
         * Prints rows by KEK.
         *
         * @param what   what each is
         * @param counts rows of each, by KEK
         * @param out    where to
         */
        private static void print(String what,
                Map<String, Map<String, Long>> counts, PrintStream out)
        {
            counts.forEach((name, byKek) ->
            {
                List<String> parts = new ArrayList<>();
                byKek.forEach((kek, count) -> parts.add(kek + " " + count));
                out.println(what + " " + name + ": " + (parts.isEmpty()
                        ? "no rows" : String.join(", ", parts)));
            });
        }
    }

    /**
     * What {@code rotate} answers.
     *
     * @param scope   the scope
     * @param from    the KEK in use before
     * @param now     the rotation in force
     * @param changed whether this run changed it
     */
    public record Rotated(String scope, String from, ControlState.Kek now,
            boolean changed)
    {
    }

    /**
     * {@code rotate}: the stage's new data is wrapped with another KEK,
     * once its pods have read it and checked they can use it.
     *
     * @param <C> the stage's context
     */
    static final class Rotate<C extends KeyAdmin> implements CliOperation<C,
            Rotate.Request, Rotated>, Answering<Rotated>
    {

        /**
         * What is asked.
         *
         * @param kek the KEK to rotate to
         */
        record Request(String kek)
        {
        }

        /** Creates the command. */
        Rotate()
        {
        }

        @Override
        public String name()
        {
            return "rotate";
        }

        @Override
        public String usage()
        {
            return """
                    rotate --kek <reference>
                        Wraps the stage's new data with this KEK from now
                        on: each pod reads it within seconds, checks it can
                        wrap and unwrap with it, and takes it at its next
                        batch. Checked here first, with the stage's rights.
                        Data already stored stays under its KEK, which still
                        unwraps it: rekey moves it. With local keys, the KEK
                        must be listed in sdh.crypto.keys.other-keks.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("kek");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.KEYS_ADMIN;
        }

        @Override
        public Kind kind()
        {
            return Kind.ACTION;
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request(kek("kek", arguments.required("kek")));
        }

        @Override
        public Rotated run(Request request, C context, Invocation invocation)
                throws Exception
        {
            ControlState state = context.controlState(true);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            String from = active(state, context);
            invocation.audit().add(Attr.KEK_FROM, from);
            invocation.audit().add(Attr.KEK_TO, request.kek());
            if (from.equals(request.kek()))
            {
                return new Rotated(state.scope(), from, state.kek().orElse(
                        new ControlState.Kek(state.scope(), from, null,
                                null)), false);
            }
            probe(context, request.kek());
            ControlState.Kek now = state.rotate(invocation.principal(),
                    request.kek());
            return new Rotated(state.scope(), from, now, true);
        }

        @Override
        public Map<String, Object> answer(Rotated result)
        {
            return Answers.object("scope", result.scope(),
                    "from", result.from(), "active", result.now().kek(),
                    "rotatedBy", result.now().setBy(),
                    "rotatedAt", result.now().setAt(),
                    "changed", result.changed());
        }

        @Override
        public void print(Rotated result, PrintStream out)
        {
            out.println(result.changed() ? "Rotated " + result.scope()
                    + " from " + result.from() + " to " + result.now().kek()
                    + "; its pods take it within seconds"
                    : result.scope() + " already wraps new data with "
                            + result.from());
        }
    }

    /**
     * Checks the stage can wrap and unwrap with a KEK.
     *
     * @param context the stage's context
     * @param kek     the KEK
     * @throws AdminArgs.UsageException if it can't, naming no stored data
     * @throws Exception                if the keys can't be loaded
     */
    static void probe(KeyAdmin context, String kek) throws Exception
    {
        KeyProvider provider = context.keyProvider();
        try
        {
            KeyMaterial.probeKek(provider, kek);
        }
        catch (GeneralSecurityException | RuntimeException refused)
        {
            throw new AdminArgs.UsageException("The stage can't wrap and"
                    + " unwrap with " + kek + " ("
                    + refused.getClass().getSimpleName() + "): grant it the"
                    + " key, or with local keys list it in"
                    + " sdh.crypto.keys.other-keks");
        }
    }

    /**
     * {@code rekey}: moves what the stage stores from one KEK to another,
     * carried out by its maintenance, on the pod holding its duties
     * ({@code RekeyPass}). It needs a second person's approval. Its run
     * asks for it, then waits until it is done or failed; the CLI's for at
     * most {@code --wait}.
     *
     * @param <C> the stage's context
     */
    static final class Rekeys<C extends KeyAdmin> implements CliOperation<C,
            Rekeys.Request, ControlState.Rekeying>,
            Answering<ControlState.Rekeying>
    {

        /**
         * What is asked.
         *
         * @param from    the old KEK
         * @param to      the new KEK
         * @param waitFor how long to wait for it; null to wait until it is
         *                done or failed
         */
        record Request(String from, String to, Duration waitFor)
        {
        }

        /** Creates the command. */
        Rekeys()
        {
        }

        @Override
        public String name()
        {
            return "rekey";
        }

        @Override
        public String usage()
        {
            return """
                    rekey --from <reference> --to <reference> [--wait <min>]
                        Moves everything the stage stores under one KEK to
                        another: its tables' wrapped keys, file by file, its
                        database rows' and its ID keys. The stage's
                        maintenance does it, on the pod holding its duties,
                        a share at a time; keys shows how far it is. Waits
                        until it is done or failed, or for --wait minutes.
                        Refused while from is the KEK in use: rotate first.
                        Needs a second person's approval through the API.
                        Disable the old KEK only once keys shows nothing
                        under it.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("from", "to", "wait");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Permission permission()
        {
            return Permission.KEYS_ADMIN;
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
            String from = kek("from", arguments.required("from"));
            String to = kek("to", arguments.required("to"));
            if (from.equals(to))
            {
                throw new AdminArgs.UsageException("--from and --to must be"
                        + " different KEKs");
            }
            Duration wait = arguments.option("wait").map(KeyCommands::minutes)
                    .orElse(null);
            return new Request(from, to, wait);
        }

        /**
         * Asks for the re-key, and waits until it is done or failed, for
         * at most the time asked. A run taken over by another control pod
         * finds the one it asked for, and waits on it.
         *
         * @throws IllegalStateException if it fails, or another re-key is
         *                               under way
         */
        @Override
        public ControlState.Rekeying run(Request request, C context,
                Invocation invocation) throws Exception
        {
            ControlState state = context.controlState(true);
            invocation.audit().add(Attr.CONTROL_SCOPE, state.scope());
            invocation.audit().add(Attr.KEK_FROM, request.from());
            invocation.audit().add(Attr.KEK_TO, request.to());
            String active = active(state, context);
            if (active.equals(request.from()))
            {
                throw new AdminArgs.UsageException(request.from() + " is the"
                        + " KEK the stage's new data is wrapped with: rotate"
                        + " first");
            }
            probe(context, request.to());
            // The API's operation, so a run taken over asks for the same;
            // the CLI's own.
            String id = invocation.audit().fields().toStrings()
                    .get(Attr.OPERATION_ID.key());
            if (id == null)
            {
                id = UUID.randomUUID().toString();
                invocation.audit().add(Attr.OPERATION_ID, id);
            }
            String asked = id;
            ControlState.Rekeying now = state.requestRekey(asked,
                    invocation.principal(), request.from(), request.to());
            long until = request.waitFor() == null ? Long.MAX_VALUE
                    : System.nanoTime() + request.waitFor().toNanos();
            while (now.state().active() && System.nanoTime() < until)
            {
                Thread.sleep(Math.min(LOOK.toMillis(), Math.max(1,
                        (until - System.nanoTime()) / 1_000_000)));
                now = state.rekeying().filter(each -> each.id().equals(
                        asked))
                        .orElseThrow(() -> new IllegalStateException("The"
                                + " re-key was replaced"));
            }
            invocation.audit().add(Attr.FILES, now.files());
            invocation.audit().add(Attr.ROWS, now.rows());
            if (now.state() == ControlState.RekeyState.FAILED)
            {
                throw new IllegalStateException("The re-key failed: "
                        + now.failure());
            }
            return now;
        }

        @Override
        public Map<String, Object> answer(ControlState.Rekeying result)
        {
            return rekeyAnswer(Optional.of(result));
        }

        @Override
        public void print(ControlState.Rekeying result, PrintStream out)
        {
            printRekey(result, out);
            if (result.state().active())
            {
                out.println("Still under way; keys shows how far it is");
            }
        }
    }

    /**
     * Reads a number of minutes.
     *
     * @param value the option's value
     * @return the minutes
     * @throws AdminArgs.UsageException if it isn't a whole number from 0
     */
    static Duration minutes(String value)
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
}
