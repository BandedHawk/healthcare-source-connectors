/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.message.Lineage;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureReason;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * A stage's commands for its fail store (STAGES.md §15.3, §15.7 step 3):
 * {@code failures} lists them, {@code decline} records an operator's choice
 * not to correct one, with a reason, and {@code reopen} takes that back when
 * new information allows a correction, {@code correct} and {@code retry}
 * submit an edit, or what failed unchanged, for the stage's workers to
 * process, and {@code export} gives what failed, encrypted to the
 * operator's key. Nothing is removed: each change is
 * an event of its own, with who, when and the reason, encrypted. None
 * prints content: a failure is named by its item, record number, error
 * code and place, and a reason is never shown.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class FailureCommands
{

    /** The most failures listed. */
    static final int LISTED = 50;

    /** Not instantiated. */
    private FailureCommands()
    {
    }

    /**
     * The commands.
     *
     * @param <C> the stage's context
     * @return {@code failures}, {@code decline}, {@code reopen},
     *         {@code correct}, {@code retry} and {@code export}
     */
    public static <C extends FailureAdmin> List<AdminCommand<C>> all()
    {
        return List.of(new ListCommand<>(), new ChangeCommand<>("decline",
                        "declined", EnumSet.of(FailureStore.State.FAILED),
                        FailureStore.State.DECLINED, """
                        decline --item-id <id> --seq <n> < reason.txt
                            Records that a failure won't be corrected (it
                            can't be, or needn't be), with the reason read
                            from standard input (kubectl exec -i), kept
                            encrypted. The failure stays in the fail store;
                            its item completes once none of its failures is
                            open. --seq -1 is the item as a whole."""),
                new ChangeCommand<>("reopen", "reopened",
                        EnumSet.of(FailureStore.State.DECLINED),
                        FailureStore.State.FAILED, """
                        reopen --item-id <id> --seq <n> < reason.txt
                            Opens a declined failure again, when new
                            information allows a correction, with the
                            reason read from standard input. Its item is
                            incomplete again until the failure is resolved
                            or declined; an item already complete is
                            reopened, here and downstream, and completes
                            again, numbered after the last."""),
                new CorrectCommand<>(), new RetryCommand<>(),
                new ExportCommand<>());
    }

    /**
     * Reads an item ID.
     *
     * @param text the ID as typed
     * @return the ID
     * @throws AdminArgs.UsageException if it isn't a UUID
     */
    public static UUID itemId(String text)
    {
        try
        {
            return UUID.fromString(text);
        }
        catch (IllegalArgumentException notUuid)
        {
            throw new AdminArgs.UsageException("--item-id must be a UUID");
        }
    }

    /**
     * Reads a record number.
     *
     * @param text the number as typed
     * @return the number
     * @throws AdminArgs.UsageException if it isn't one
     */
    static long seq(String text)
    {
        try
        {
            long seq = Long.parseLong(text);
            if (seq < FailureStore.WHOLE_ITEM)
            {
                throw new NumberFormatException();
            }
            return seq;
        }
        catch (NumberFormatException notNumber)
        {
            throw new AdminArgs.UsageException(
                    "--seq must be a record number, or -1 for the item");
        }
    }

    /**
     * Reads an operator's reason from its input: at most
     * {@link FailureReason#MAX_BYTES} bytes, not empty.
     *
     * @param input the input
     * @return the reason
     * @throws IOException              if it can't be read
     * @throws AdminArgs.UsageException if it is empty or too long
     */
    public static String reason(InputStream input) throws IOException
    {
        byte[] bytes = input.readNBytes(FailureReason.MAX_BYTES + 1);
        if (bytes.length > FailureReason.MAX_BYTES)
        {
            throw new AdminArgs.UsageException("The reason is longer than "
                    + FailureReason.MAX_BYTES + " bytes");
        }
        String text = new String(bytes, StandardCharsets.UTF_8).strip();
        if (text.isEmpty())
        {
            throw new AdminArgs.UsageException("Give the reason on standard"
                    + " input, such as < reason.txt with kubectl exec -i");
        }
        return text;
    }

    /**
     * Lists failures: the open ones, newest first; with {@code --all},
     * every state; with {@code --item-id}, one item's, each with its
     * changes.
     *
     * @param <C> the stage's context
     */
    static final class ListCommand<C extends FailureAdmin>
            implements CliOperation<C, ListCommand.Request,
                    ListCommand.Result>
    {

        /**
         * What is asked.
         *
         * @param all    every state, not only the open
         * @param itemId one item's failures, with their changes; null for
         *               the stage's
         */
        record Request(boolean all, UUID itemId)
        {
        }

        /**
         * What was found.
         *
         * @param stage  the stage
         * @param itemId the item asked for; null for the stage's
         * @param item   the item's failures, each with its changes; null
         *               for the stage's
         * @param open   how many are open, and the oldest; null for an
         *               item
         * @param listed the stage's failures listed; null for an item
         */
        record Result(String stage, UUID itemId, List<ItemFailure> item,
                FailureStore.Open open, List<FailureStore.Listed> listed)
        {
        }

        /**
         * One of an item's failures, with its own changes and corrections.
         *
         * @param failure     the failure
         * @param events      its changes of state
         * @param corrections its corrections and retries
         */
        record ItemFailure(FailureStore.Listed failure,
                List<FailureStore.Event> events,
                List<CorrectionStore.Listed> corrections)
        {
        }

        /** Creates the command. */
        ListCommand()
        {
        }

        @Override
        public String name()
        {
            return "failures";
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
        public String usage()
        {
            return """
                    failures [--all] [--item-id <id>]
                        Lists the stage's open failures, newest first (the
                        first %d); --all lists every state; --item-id lists
                        one item's, with each change of state (who, when,
                        and whether a reason was given; never the reason)
                        and each correction.
                        Shows no content. Reads only."""
                    .formatted(LISTED);
        }

        @Override
        public Set<String> options()
        {
            return Set.of("item-id");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of("all");
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request(arguments.flag("all"), arguments
                    .option("item-id").map(FailureCommands::itemId)
                    .orElse(null));
        }

        @Override
        public Result run(Request request, C context, Invocation invocation)
                throws Exception
        {
            AuditRecord audit = invocation.audit();
            audit.add(Attr.STAGE, context.stage());
            FailureStore failures = context.failures();
            UUID itemId = request.itemId();
            if (itemId == null)
            {
                List<FailureStore.Listed> listed = failures.list(
                        !request.all(), LISTED);
                audit.add(Attr.COUNT, listed.size());
                return new Result(context.stage(), null, null,
                        failures.open(), listed);
            }
            audit.add(Attr.ITEM_ID, itemId);
            List<FailureStore.Listed> listed = failures.listItem(itemId);
            audit.add(Attr.COUNT, listed.size());
            int place = Lineage.stageOf(context.stage());
            List<ItemFailure> item = new ArrayList<>();
            for (FailureStore.Listed failure : listed)
            {
                FailureStore.Failure what = failure.stored().failure();
                // A number's failures, one per revision, share its events
                // and corrections: each shows its own.
                List<FailureStore.Event> events = new ArrayList<>();
                for (FailureStore.Event event : failures.events(itemId,
                        what.seq()))
                {
                    if (event.lineage().equals(what.lineage()))
                    {
                        events.add(event);
                    }
                }
                List<CorrectionStore.Listed> corrections = new ArrayList<>();
                CorrectionStore store = context.corrections();
                for (CorrectionStore.Listed correction : store == null
                        ? List.<CorrectionStore.Listed>of()
                        : store.list(itemId, what.seq()))
                {
                    if (correction.lineage() == null
                            ? first(listed, failure)
                            : correction.lineage().equals(what.lineage()
                                    .nextRevision(place)))
                    {
                        corrections.add(correction);
                    }
                }
                item.add(new ItemFailure(failure, events, corrections));
            }
            return new Result(context.stage(), itemId, item, null, null);
        }

        @Override
        public void print(Result result, PrintStream out)
        {
            if (result.itemId() == null)
            {
                FailureStore.Open open = result.open();
                out.println(open.count() + " open at the " + result.stage()
                        + " stage" + (open.oldest() == null ? ""
                                : ", the oldest failed " + open.oldest()));
                for (FailureStore.Listed failure : result.listed())
                {
                    out.println(line(failure));
                }
                return;
            }
            if (result.item().isEmpty())
            {
                out.println("no failures for " + result.itemId() + " at the "
                        + result.stage() + " stage");
            }
            for (ItemFailure each : result.item())
            {
                out.println(line(each.failure()));
                for (FailureStore.Event event : each.events())
                {
                    out.println("  " + event.at() + " "
                            + (event.resentBy() == null
                                    ? event.from() + " -> " + event.to()
                                    : "resent as " + event.resentBy())
                            + " by " + event.operator()
                            + (event.reason() == null ? ""
                                    : " (reason kept, encrypted)"));
                }
                for (CorrectionStore.Listed correction : each.corrections())
                {
                    out.println("  " + (correction.kind()
                            == CorrectionStore.Kind.RETRY ? "retry "
                                    : "correction ")
                            + correction.correction()
                            + " " + correction.state() + " by "
                            + correction.submittedBy() + " at "
                            + correction.submittedAt() + ", "
                            + correction.length() + " bytes"
                            + (correction.errorCode() == null ? ""
                                    : ", " + correction.errorCode())
                            + (correction.lineage() == null ? ""
                                    : ", as " + correction.lineage())
                            + (correction.recordId() == null ? ""
                                    : ", made " + correction.recordId()));
                }
            }
        }

        /**
         * Whether a failure is the first at its number, the one a
         * correction submitted before revisions were noted was for.
         *
         * @param listed  the item's failures
         * @param failure one of them
         * @return true if none at its number failed before it
         */
        private static boolean first(List<FailureStore.Listed> listed,
                FailureStore.Listed failure)
        {
            long seq = failure.stored().failure().seq();
            return listed.stream().filter(other -> other.stored().failure()
                    .seq() == seq).noneMatch(other -> other.failedAt()
                            .isBefore(failure.failedAt()));
        }

        /**
         * A failure, on one line.
         *
         * @param failure the failure
         * @return item, record number, lineage (STAGES.md §15.8.8), state,
         *         error code, place, version, when it failed, who changed
         *         it last, and how often an exact copy of the item came in
         *         while it was open (§15.5a)
         */
        private static String line(FailureStore.Listed failure)
        {
            FailureStore.Failure what = failure.stored().failure();
            return what.itemId() + " seq=" + what.seq() + " "
                    + what.lineage() + " "
                    + failure.stored().state() + " " + what.errorCode()
                    + " offset=" + what.sourceOffset() + " failed="
                    + failure.failedAt()
                    + (failure.changedBy() == null ? ""
                            : " changed=" + failure.changedAt() + " by "
                                    + failure.changedBy())
                    + (what.version() == null ? ""
                            : " version=" + what.version())
                    + (failure.resentCount() == 0 ? ""
                            : " resent=" + failure.resentCount() + " last="
                                    + failure.resentAt());
        }
    }


    /**
     * Changes a failure's state, with a reason, then has the stage act on
     * it.
     *
     * @param <C> the stage's context
     */
    static final class ChangeCommand<C extends FailureAdmin>
            implements CliOperation<C, ChangeCommand.Request,
                    ChangeCommand.Result>
    {

        /**
         * What is asked.
         *
         * @param itemId the failure's item
         * @param seq    its record number; -1 for the item as a whole
         * @param reason why, kept encrypted
         */
        record Request(UUID itemId, long seq, Given<String> reason)
        {
        }

        /**
         * What was done.
         *
         * @param stage  the stage
         * @param itemId the failure's item
         * @param seq    its record number
         * @param before its state before
         * @param next   what the stage does next, as the context says;
         *               its failure, if it failed, after the change made
         */
        record Result(String stage, UUID itemId, long seq,
                FailureStore.State before, Partial<String> next)
        {
        }

        /** The command's name. */
        private final String name;

        /** What the change is called in its event, such as declined. */
        private final String action;

        /** The states the failure may be changed from. */
        private final Set<FailureStore.State> from;

        /** The state it is changed to. */
        private final FailureStore.State to;

        /** The command's help. */
        private final String usage;

        /**
         * Creates the command.
         *
         * @param name   its name
         * @param action what the change is called in its event
         * @param from   the states it changes from
         * @param to     the state it changes to
         * @param usage  its help
         */
        ChangeCommand(String name, String action,
                Set<FailureStore.State> from, FailureStore.State to,
                String usage)
        {
            this.name = name;
            this.action = action;
            this.from = from;
            this.to = to;
            this.usage = usage;
        }

        @Override
        public String name()
        {
            return name;
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
        public String usage()
        {
            return usage;
        }

        @Override
        public Set<String> options()
        {
            return Set.of("item-id", "seq");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request(itemId(arguments.required("item-id")),
                    seq(arguments.required("seq")),
                    () -> reason(context.reasonInput()));
        }

        @Override
        public Result run(Request request, C context, Invocation invocation)
                throws Exception
        {
            AuditRecord audit = invocation.audit();
            UUID itemId = request.itemId();
            long seq = request.seq();
            audit.add(Attr.STAGE, context.stage());
            audit.add(Attr.ITEM_ID, itemId);
            audit.add(Attr.SEQ, seq);
            String text = request.reason().get();
            FailureStore failures = context.failuresToChange();
            KeyProvider keys = context.reasonKeys();
            String kek = context.reasonKek();
            FailureStore.Changed changed;
            try
            {
                changed = failures.change(itemId, seq, from, to, action,
                        audit.operator(), event -> FailureReason.seal(keys,
                                kek, itemId, seq, event, text));
            }
            catch (FailureStore.RefusedException refused)
            {
                throw new IllegalStateException(refused.getMessage());
            }
            audit.add(Attr.FROM_STATE, changed.before().state().name());
            audit.add(Attr.STATE, to.name());
            return new Result(context.stage(), itemId, seq,
                    changed.before().state(),
                    Partial.of(() -> context.changed(itemId, audit)));
        }

        @Override
        public void print(Result result, PrintStream out) throws Exception
        {
            out.println(action + " failure " + result.seq() + " of "
                    + result.itemId() + " at the " + result.stage()
                    + " stage: " + result.before() + " -> " + to
                    + ", the reason kept encrypted");
            String next = result.next().get();
            if (next != null)
            {
                out.println(next);
            }
        }
    }

    /**
     * Reads what an operator gives on standard input, up to a limit.
     *
     * @param input the input
     * @param max   the most bytes taken
     * @param what  what it is, for an error
     * @return the bytes
     * @throws IOException              if it can't be read
     * @throws AdminArgs.UsageException if it is empty or too long
     */
    public static byte[] input(InputStream input, long max, String what)
            throws IOException
    {
        byte[] bytes = input.readNBytes((int) Math.min(Integer.MAX_VALUE - 8,
                max + 1));
        if (bytes.length > max)
        {
            Arrays.fill(bytes, (byte) 0);
            throw new AdminArgs.UsageException("The " + what
                    + " is longer than " + max + " bytes");
        }
        if (bytes.length == 0)
        {
            throw new AdminArgs.UsageException("Give the " + what
                    + " on standard input, with kubectl exec -i");
        }
        return bytes;
    }

    /**
     * Submits an operator's correction of a failure (STAGES.md §15.6c):
     * the edited input, read from standard input, stored encrypted, and the
     * failure moved to {@code CORRECTION_PENDING}. The stage's workers
     * process it as any input.
     *
     * @param <C> the stage's context
     */
    static final class CorrectCommand<C extends FailureAdmin>
            implements CliOperation<C, CorrectCommand.Request, Submitted>
    {

        /**
         * What is asked.
         *
         * @param itemId  the failure's item
         * @param seq     its record number
         * @param content the corrected record, read only once the stage is
         *                known to take corrections
         */
        record Request(UUID itemId, long seq, Given<byte[]> content)
        {
        }

        /** Creates the command. */
        CorrectCommand()
        {
        }

        @Override
        public String name()
        {
            return "correct";
        }

        @Override
        public Permission permission()
        {
            return Permission.PIPELINE_OPERATE;
        }

        /**
         * An operator's permission, and PHI export's: whoever corrects a
         * record has read it (CONTROL-API-DESIGN.md §4).
         *
         * @param request the request
         * @return both
         */
        @Override
        public Set<Permission> permissions(Request request)
        {
            return EnumSet.of(Permission.PIPELINE_OPERATE,
                    Permission.PHI_EXPORT);
        }

        @Override
        public Kind kind()
        {
            return Kind.ACTION;
        }

        @Override
        public String usage()
        {
            return """
                    correct --item-id <id> --seq <n> < corrected-record
                        Submits a corrected record for a failure that is
                        FAILED or FAILED_CORRECTION, read from standard
                        input (kubectl exec -i), kept encrypted. The
                        stage's workers process it within a renewal
                        interval, as the record would have been: if it
                        passes, it is stored as a new record linked to the
                        failed one and sent on, and the failure is
                        CORRECTED; if not, FAILED_CORRECTION. Nothing of
                        it is printed.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("item-id", "seq");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request(itemId(arguments.required("item-id")),
                    seq(arguments.required("seq")),
                    () -> input(context.reasonInput(),
                            context.maxCorrectionBytes(),
                            "corrected record"));
        }

        @Override
        public Submitted run(Request request, C context,
                Invocation invocation) throws Exception
        {
            AuditRecord audit = invocation.audit();
            UUID itemId = request.itemId();
            long seq = request.seq();
            audit.add(Attr.STAGE, context.stage());
            audit.add(Attr.ITEM_ID, itemId);
            audit.add(Attr.SEQ, seq);
            if (!context.corrects())
            {
                throw new IllegalStateException("The " + context.stage()
                        + " stage doesn't take corrections yet");
            }
            byte[] content = request.content().get();
            int length = content.length;
            int number;
            try
            {
                number = context.correctionsToChange().submit(itemId, seq,
                        content, context.reasonKeys(), context.reasonKek(),
                        audit.operator());
            }
            catch (FailureStore.RefusedException refused)
            {
                throw new IllegalStateException(refused.getMessage());
            }
            finally
            {
                Arrays.fill(content, (byte) 0);
            }
            audit.add(Attr.CORRECTION, number);
            audit.add(Attr.BYTES, length);
            audit.add(Attr.STATE,
                    FailureStore.State.REVISION_PENDING.name());
            return new Submitted(context.stage(), itemId, seq, number,
                    length);
        }

        @Override
        public void print(Submitted result, PrintStream out)
        {
            out.println("submitted correction " + result.number()
                    + " of failure " + result.seq() + " of "
                    + result.itemId() + " at the " + result.stage()
                    + " stage (" + result.length() + " bytes, kept"
                    + " encrypted); the stage's workers process it within a"
                    + " renewal interval: see failures --item-id");
        }
    }

    /**
     * A correction or retry submitted.
     *
     * @param stage  the stage
     * @param itemId the failure's item
     * @param seq    its record number
     * @param number the correction's number
     * @param length its length, in bytes
     */
    record Submitted(String stage, UUID itemId, long seq, int number,
            int length)
    {
    }

    /**
     * Submits a retry of a failure (STAGES.md §15.6e): what it failed on,
     * read by the stage as {@code export} reads it, unchanged, as a
     * correction of kind {@code RETRY}. The stage's workers process it with
     * the version running now: if it passes the failure is {@code RETRIED},
     * and if not it is {@code FAILED} again.
     *
     * @param <C> the stage's context
     */
    static final class RetryCommand<C extends FailureAdmin>
            implements CliOperation<C, Failure, Submitted>
    {

        /** Creates the command. */
        RetryCommand()
        {
        }

        @Override
        public String name()
        {
            return "retry";
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
        public String usage()
        {
            return """
                    retry --item-id <id> --seq <n>
                        Processes again, unchanged, what a failure that is
                        FAILED or FAILED_CORRECTION failed on: for a failure
                        that wasn't the data's (a limit raised, the stage
                        fixed, a service back). The stage's workers process
                        it within a renewal interval, under the version
                        running now: if it passes, it is stored as a new
                        record linked to the failed one and sent on, and
                        the failure is RETRIED; if not, it is FAILED again,
                        and the retry's error is shown by failures
                        --item-id. Nothing of it is printed.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("item-id", "seq");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Failure bind(AdminArgs arguments, C context)
        {
            return Failure.of(arguments);
        }

        @Override
        public Submitted run(Failure request, C context,
                Invocation invocation) throws Exception
        {
            AuditRecord audit = invocation.audit();
            UUID itemId = request.itemId();
            long seq = request.seq();
            audit.add(Attr.STAGE, context.stage());
            audit.add(Attr.ITEM_ID, itemId);
            audit.add(Attr.SEQ, seq);
            if (!context.corrects())
            {
                throw new IllegalStateException("The " + context.stage()
                        + " stage doesn't take retries yet");
            }
            FailureStore.Stored failure = context.failures()
                    .failure(itemId, seq)
                    .orElseThrow(() -> new IllegalStateException("The "
                            + context.stage() + " stage has no failure "
                            + seq + " for this item"));
            if (!CorrectionStore.CORRECTABLE.contains(failure.state()))
            {
                // Refused before what failed is read.
                throw new IllegalStateException("The failure is "
                        + failure.state() + ": it can be retried only from "
                        + CorrectionStore.CORRECTABLE);
            }
            byte[] content = context.failedContent(itemId, seq);
            int length = content.length;
            int number;
            try
            {
                number = context.correctionsToChange().submit(itemId, seq,
                        content, context.reasonKeys(), context.reasonKek(),
                        audit.operator(), CorrectionStore.Kind.RETRY);
            }
            catch (FailureStore.RefusedException refused)
            {
                throw new IllegalStateException(refused.getMessage());
            }
            finally
            {
                Arrays.fill(content, (byte) 0);
            }
            audit.add(Attr.CORRECTION, number);
            audit.add(Attr.BYTES, length);
            audit.add(Attr.STATE,
                    FailureStore.State.REVISION_PENDING.name());
            return new Submitted(context.stage(), itemId, seq, number,
                    length);
        }

        @Override
        public void print(Submitted result, PrintStream out)
        {
            out.println("submitted retry " + result.number() + " of failure "
                    + result.seq() + " of " + result.itemId() + " at the "
                    + result.stage() + " stage (" + result.length()
                    + " bytes, kept encrypted); the stage's workers process"
                    + " it within a renewal interval: see failures"
                    + " --item-id");
        }
    }

    /**
     * A failure, by its item and record number.
     *
     * @param itemId the item
     * @param seq    the record number; -1 for the item as a whole
     */
    record Failure(UUID itemId, long seq)
    {

        /**
         * The failure an operator's options name.
         *
         * @param arguments the options
         * @return the failure
         * @throws AdminArgs.UsageException if they don't name one
         */
        static Failure of(AdminArgs arguments)
        {
            return new Failure(FailureCommands.itemId(
                    arguments.required("item-id")),
                    FailureCommands.seq(arguments.required("seq")));
        }
    }

    /**
     * Exports what a failure failed on, encrypted to the operator's public
     * key, read from standard input, so it can be corrected (STAGES.md
     * §15.6c). The plaintext never leaves the pod.
     *
     * @param <C> the stage's context
     */
    static final class ExportCommand<C extends FailureAdmin>
            implements CliOperation<C, ExportCommand.Request, Integer>
    {

        /** What each line of output starts with. */
        static final String PREFIX = ExportSealer.LINE_PREFIX;

        /**
         * What is asked.
         *
         * @param itemId    the failure's item
         * @param seq       its record number
         * @param publicKey the key to seal to, PEM
         */
        record Request(UUID itemId, long seq, Given<byte[]> publicKey)
        {
        }

        /** Creates the command. */
        ExportCommand()
        {
        }

        @Override
        public String name()
        {
            return "export";
        }

        @Override
        public Permission permission()
        {
            return Permission.PHI_EXPORT;
        }

        @Override
        public Kind kind()
        {
            return Kind.SEALED;
        }

        @Override
        public String usage()
        {
            return """
                    export --item-id <id> --seq <n> < operator-public.pem
                        Prints what a failure failed on, encrypted (JWE) to
                        the public key read from standard input (EC P-256
                        or RSA, PEM), on a line starting "sdh-export: ".
                        Only the holder of the private key can read it.
                        Audited with the key's SHA-256.""";
        }

        @Override
        public Set<String> options()
        {
            return Set.of("item-id", "seq");
        }

        @Override
        public Set<String> flags()
        {
            return Set.of();
        }

        @Override
        public Request bind(AdminArgs arguments, C context)
        {
            return new Request(itemId(arguments.required("item-id")),
                    seq(arguments.required("seq")),
                    () -> input(context.reasonInput(),
                            ExportSealer.MAX_KEY_BYTES, "public key"));
        }

        /**
         * Seals what the failure failed on to the sink.
         *
         * @return the bytes sealed
         */
        @Override
        public Integer run(Request request, C context, Invocation invocation)
                throws Exception
        {
            AuditRecord audit = invocation.audit();
            UUID itemId = request.itemId();
            long seq = request.seq();
            audit.add(Attr.STAGE, context.stage());
            audit.add(Attr.ITEM_ID, itemId);
            audit.add(Attr.SEQ, seq);
            ExportSealer sealer = new ExportSealer(request.publicKey().get());
            audit.add(Attr.RECIPIENT_KEY, sealer.fingerprint());
            if (context.failures().failure(itemId, seq).isEmpty())
            {
                throw new IllegalStateException("The " + context.stage()
                        + " stage has no failure " + seq + " for this item");
            }
            byte[] content = context.failedContent(itemId, seq);
            int length = content.length;
            String sealed;
            try
            {
                sealed = sealer.seal(content);
            }
            finally
            {
                Arrays.fill(content, (byte) 0);
            }
            audit.add(Attr.BYTES, length);
            invocation.sink().sealed(sealed);
            return length;
        }

        /**
         * Prints nothing more: the sealed line went to the sink.
         *
         * @param result the bytes sealed
         * @param out    unused
         */
        @Override
        public void print(Integer result, PrintStream out)
        {
        }
    }
}
