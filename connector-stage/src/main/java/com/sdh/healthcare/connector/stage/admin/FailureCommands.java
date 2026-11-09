/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.core.failure.CorrectionStore;
import com.sdh.healthcare.connector.core.failure.FailureReason;
import com.sdh.healthcare.connector.core.failure.FailureStore;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
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
                        "declined", EnumSet.of(FailureStore.State.FAILED,
                                FailureStore.State.FAILED_CORRECTION),
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
            implements AdminCommand<C>
    {

        @Override
        public String name()
        {
            return "failures";
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
        public void run(AdminArgs arguments, C context, AuditRecord audit,
                PrintStream out) throws Exception
        {
            audit.add(Attr.STAGE, context.stage());
            FailureStore failures = context.failures();
            if (arguments.option("item-id").isPresent())
            {
                UUID itemId = itemId(arguments.required("item-id"));
                audit.add(Attr.ITEM_ID, itemId);
                List<FailureStore.Listed> listed = failures.listItem(itemId);
                audit.add(Attr.COUNT, listed.size());
                if (listed.isEmpty())
                {
                    out.println("no failures for " + itemId + " at the "
                            + context.stage() + " stage");
                }
                for (FailureStore.Listed failure : listed)
                {
                    out.println(line(failure));
                    for (FailureStore.Event event : failures.events(itemId,
                            failure.stored().failure().seq()))
                    {
                        out.println("  " + event.at() + " " + event.from()
                                + " -> " + event.to() + " by "
                                + event.operator()
                                + (event.reason() == null ? ""
                                        : " (reason kept, encrypted)"));
                    }
                    CorrectionStore corrections = context.corrections();
                    for (CorrectionStore.Listed correction
                            : corrections == null
                                    ? List.<CorrectionStore.Listed>of()
                                    : corrections.list(itemId,
                                            failure.stored().failure()
                                                    .seq()))
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
                                + (correction.recordId() == null ? ""
                                        : ", made " + correction.recordId()));
                    }
                }
                return;
            }
            boolean all = arguments.flag("all");
            List<FailureStore.Listed> listed = failures.list(!all, LISTED);
            audit.add(Attr.COUNT, listed.size());
            FailureStore.Open open = failures.open();
            out.println(open.count() + " open at the " + context.stage()
                    + " stage" + (open.oldest() == null ? ""
                            : ", the oldest failed " + open.oldest()));
            for (FailureStore.Listed failure : listed)
            {
                out.println(line(failure));
            }
        }

        /**
         * A failure, on one line.
         *
         * @param failure the failure
         * @return item, record number, state, error code, place, version,
         *         when it failed, and who changed it last
         */
        private static String line(FailureStore.Listed failure)
        {
            FailureStore.Failure what = failure.stored().failure();
            return what.itemId() + " seq=" + what.seq() + " "
                    + failure.stored().state() + " " + what.errorCode()
                    + " offset=" + what.sourceOffset() + " failed="
                    + failure.failedAt()
                    + (failure.changedBy() == null ? ""
                            : " changed=" + failure.changedAt() + " by "
                                    + failure.changedBy())
                    + (what.version() == null ? ""
                            : " version=" + what.version());
        }
    }

    /**
     * Changes a failure's state, with a reason, then has the stage act on
     * it.
     *
     * @param <C> the stage's context
     */
    static final class ChangeCommand<C extends FailureAdmin>
            implements AdminCommand<C>
    {

        /** The command's name. */
        private final String name;

        /** The audit event's last part. */
        private final String action;

        /** The states it changes from. */
        private final Set<FailureStore.State> from;

        /** The state it changes to. */
        private final FailureStore.State to;

        /** Its usage. */
        private final String usage;

        /**
         * Creates the command.
         *
         * @param name   its name
         * @param action the audit event's last part
         * @param from   the states it changes from
         * @param to     the state it changes to
         * @param usage  its usage
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
        public void run(AdminArgs arguments, C context, AuditRecord audit,
                PrintStream out) throws Exception
        {
            UUID itemId = itemId(arguments.required("item-id"));
            long seq = seq(arguments.required("seq"));
            audit.add(Attr.STAGE, context.stage());
            audit.add(Attr.ITEM_ID, itemId);
            audit.add(Attr.SEQ, seq);
            String text = reason(context.reasonInput());
            FailureStore failures = context.failuresToChange();
            KeyProvider keys = context.reasonKeys();
            String kek = context.reasonKek();
            FailureStore.Changed changed;
            try
            {
                changed = failures.change(itemId, seq, from, to, action,
                        audit.operator(), event -> FailureReason.seal(keys, kek,
                                itemId, seq, event, text));
            }
            catch (FailureStore.RefusedException refused)
            {
                throw new IllegalStateException(refused.getMessage());
            }
            audit.add(Attr.FROM_STATE, changed.before().state().name());
            audit.add(Attr.STATE, to.name());
            out.println(action + " failure " + seq + " of " + itemId
                    + " at the " + context.stage() + " stage: "
                    + changed.before().state() + " -> " + to
                    + ", the reason kept encrypted");
            context.changed(itemId, audit, out);
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
    static byte[] input(InputStream input, long max, String what)
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
            implements AdminCommand<C>
    {

        @Override
        public String name()
        {
            return "correct";
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
        public void run(AdminArgs arguments, C context, AuditRecord audit,
                PrintStream out) throws Exception
        {
            UUID itemId = itemId(arguments.required("item-id"));
            long seq = seq(arguments.required("seq"));
            audit.add(Attr.STAGE, context.stage());
            audit.add(Attr.ITEM_ID, itemId);
            audit.add(Attr.SEQ, seq);
            if (!context.corrects())
            {
                throw new IllegalStateException("The " + context.stage()
                        + " stage doesn't take corrections yet");
            }
            byte[] content = input(context.reasonInput(),
                    context.maxCorrectionBytes(), "corrected record");
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
                    FailureStore.State.CORRECTION_PENDING.name());
            out.println("submitted correction " + number + " of failure "
                    + seq + " of " + itemId + " at the " + context.stage()
                    + " stage (" + length + " bytes, kept encrypted); the"
                    + " stage's workers process it within a renewal"
                    + " interval: see failures --item-id");
        }
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
            implements AdminCommand<C>
    {

        @Override
        public String name()
        {
            return "retry";
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
        public void run(AdminArgs arguments, C context, AuditRecord audit,
                PrintStream out) throws Exception
        {
            UUID itemId = itemId(arguments.required("item-id"));
            long seq = seq(arguments.required("seq"));
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
                    FailureStore.State.CORRECTION_PENDING.name());
            out.println("submitted retry " + number + " of failure " + seq
                    + " of " + itemId + " at the " + context.stage()
                    + " stage (" + length + " bytes, kept encrypted); the"
                    + " stage's workers process it within a renewal"
                    + " interval: see failures --item-id");
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
            implements AdminCommand<C>
    {

        /** What starts the line the export is printed on. */
        static final String PREFIX = "sdh-export: ";

        @Override
        public String name()
        {
            return "export";
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
        public void run(AdminArgs arguments, C context, AuditRecord audit,
                PrintStream out) throws Exception
        {
            UUID itemId = itemId(arguments.required("item-id"));
            long seq = seq(arguments.required("seq"));
            audit.add(Attr.STAGE, context.stage());
            audit.add(Attr.ITEM_ID, itemId);
            audit.add(Attr.SEQ, seq);
            ExportSealer sealer = new ExportSealer(input(
                    context.reasonInput(), ExportSealer.MAX_KEY_BYTES,
                    "public key"));
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
            out.println(PREFIX + sealed);
        }
    }
}
