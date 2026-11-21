/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import com.sdh.healthcare.connector.contract.table.RecordsTable;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.stage.admin.AdminArgs;
import com.sdh.healthcare.connector.stage.admin.AuditRecord;
import com.sdh.healthcare.connector.stage.admin.CliOperation;
import com.sdh.healthcare.connector.stage.admin.ExportSealer;
import com.sdh.healthcare.connector.stage.admin.FailureCommands;
import com.sdh.healthcare.connector.stage.admin.Given;
import com.sdh.healthcare.connector.stage.admin.Invocation;
import com.sdh.healthcare.connector.stage.admin.Permission;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * {@code restore}: decrypts a landed item, whole or from an offset, into a
 * directory (design §11.4). The item is read from the landing node's
 * {@value LandingTables#LANDED_ITEMS}, and its digest is checked over the
 * whole item even when only its tail is written; on a mismatch nothing is
 * left behind.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class RestoreCommand implements CliOperation<SplitAdminContext,
        RestoreCommand.Request, RestoreCommand.Result>
{

    /** The command's help. */
    static final String USAGE = """
            restore --item-id <id> --to <dir>
                    [--from-offset <n> | --unprocessed] [--original-name]
                    (< operator-public.pem | --plaintext --ticket <ref>)
                Decrypts a landed item, checks its digest, and writes it
                into <dir> sealed (JWE) to the public key read from
                standard input (EC P-256 or RSA, PEM): <id>.jwe, one JWE
                a line, each a part of up to 8 MiB in order, numbered in
                its protected header. Only the key's holder can read it.
                --from-offset   start at this byte offset instead of 0
                --unprocessed   start after the item's last split record:
                                only its unprocessed tail (headers and
                                envelopes before that point aren't repeated)
                --original-name name the output after the original file
                                instead of <id>
                --plaintext     write it unsealed, as plaintext, for a
                                break-glass case that needs it: requires
                                --ticket, the change or incident it is for
                                (letters, digits, . _ : / - up to 64),
                                which is audited""";

    /** A ticket reference: a reference, never free text. */
    private static final Pattern TICKET =
            Pattern.compile("[A-Za-z0-9._:/-]{1,64}");

    /** How much plaintext goes in one sealed part of a file. */
    static final int PART_BYTES = 8 << 20;

    /**
     * How much plaintext goes in one sealed part sent to the control API's
     * caller (CONTROL-API-DESIGN.md §7): the most the API holds at once.
     */
    static final int CALLER_PART_BYTES = 1 << 20;

    /**
     * What is asked.
     *
     * @param itemId       the item
     * @param destination  where the output is written; null to send it
     *                     to the control API's caller as sealed parts
     * @param plaintext    whether to write it unsealed, for break-glass
     * @param ticket       the change or incident a plaintext restore is
     *                     for; null otherwise
     * @param publicKey    the key to seal to, PEM; read only for a sealed
     *                     restore
     * @param fromOffset   the byte offset to start at; null for 0
     * @param unprocessed  whether to start after the item's last split
     *                     record
     * @param originalName whether to name the output after the original
     *                     file
     */
    record Request(UUID itemId, Destination destination, boolean plaintext,
            String ticket, Given<byte[]> publicKey, Long fromOffset,
            boolean unprocessed, boolean originalName)
    {
    }

    /** Where a restore's output goes. */
    interface Destination
    {

        /**
         * Writes the output, as a new file, only once it is whole.
         *
         * @param name   its name
         * @param writer writes it
         * @return the bytes written
         * @throws Exception if it can't be written; nothing is left
         */
        long write(String name, OutputFiles.Writer writer) throws Exception;

        /**
         * Where an output is, to say so.
         *
         * @param name its name; null for where outputs go, as when its
         *             name is the original's, which is PHI
         * @return the place
         */
        String where(String name);
    }

    /**
     * What was restored.
     *
     * @param restored  the bytes restored, before sealing
     * @param from      the offset it started at
     * @param size      the item's size
     * @param plaintext whether it was written unsealed
     * @param where     where it was written, without the original name
     */
    record Result(long restored, long from, long size, boolean plaintext,
            String where)
    {
    }

    /** Creates the command. */
    RestoreCommand()
    {
    }

    @Override
    public Permission permission()
    {
        return Permission.PHI_EXPORT;
    }

    /**
     * A plaintext restore needs break-glass; a sealed one, PHI export.
     *
     * @param request the request
     * @return the permission
     */
    @Override
    public Set<Permission> permissions(Request request)
    {
        return EnumSet.of(request.plaintext() ? Permission.PHI_BREAK_GLASS
                : Permission.PHI_EXPORT);
    }

    @Override
    public Kind kind()
    {
        return Kind.SEALED;
    }

    /**
     * A plaintext restore needs a second person (CONTROL-API-DESIGN.md
     * §6); a sealed one doesn't.
     *
     * @param request the request
     * @return the approval needed
     */
    @Override
    public Approval approval(Request request)
    {
        return request.plaintext() ? Approval.FOUR_EYES : Approval.NONE;
    }

    @Override
    public String name()
    {
        return "restore";
    }

    @Override
    public String usage()
    {
        return USAGE;
    }

    @Override
    public Set<String> flags()
    {
        return Set.of("unprocessed", "original-name", "plaintext");
    }

    @Override
    public Set<String> options()
    {
        return Set.of("item-id", "to", "from-offset", "ticket");
    }

    @Override
    public Request bind(AdminArgs arguments, SplitAdminContext context)
    {
        UUID itemId = parseId(arguments.required("item-id"));
        Long fromOffset = arguments.option("from-offset")
                .map(RestoreCommand::parseOffset).orElse(null);
        if (arguments.outputToCaller())
        {
            if (arguments.flag("plaintext") || arguments.flag("original-name")
                    || arguments.option("to").isPresent())
            {
                throw new AdminArgs.UsageException("The control API's"
                        + " restore is sealed to the caller, under the"
                        + " item's ID");
            }
            return new Request(itemId, null, false, null,
                    () -> FailureCommands.input(arguments.input(
                            () -> System.in), ExportSealer.MAX_KEY_BYTES,
                            "public key"), fromOffset,
                    arguments.flag("unprocessed"), false);
        }
        Path directory = OutputFiles.directory(arguments.required("to"));
        return new Request(itemId, new Destination()
        {
            @Override
            public long write(String name, OutputFiles.Writer writer)
                    throws Exception
            {
                return OutputFiles.writeNew(directory, name, writer);
            }

            @Override
            public String where(String name)
            {
                return name == null ? directory.toString()
                        : directory.resolve(name).toString();
            }
        }, arguments.flag("plaintext"), arguments.option("ticket")
                .orElse(null), () -> FailureCommands.input(arguments.input(
                        () -> System.in), ExportSealer.MAX_KEY_BYTES,
                        "public key"), fromOffset,
                arguments.flag("unprocessed"),
                arguments.flag("original-name"));
    }

    @Override
    public Result run(Request request, SplitAdminContext context,
            Invocation invocation) throws Exception
    {
        AuditRecord audit = invocation.audit();
        UUID itemId = request.itemId();
        audit.add(Attr.ITEM_ID, itemId);
        boolean plaintext = request.plaintext();
        ExportSealer sealer = null;
        if (plaintext)
        {
            String ticket = request.ticket();
            if (ticket == null)
            {
                throw new AdminArgs.UsageException("--plaintext requires"
                        + " --ticket <ref>, the change or incident it"
                        + " is for");
            }
            if (!TICKET.matcher(ticket).matches())
            {
                throw new AdminArgs.UsageException("--ticket must be a"
                        + " reference: letters, digits and . _ : / -, up"
                        + " to 64");
            }
            audit.add(Attr.TICKET, ticket);
            audit.add(Attr.OUTCOME, "plaintext");
        }
        else
        {
            if (request.ticket() != null)
            {
                throw new AdminArgs.UsageException(
                        "--ticket goes with --plaintext");
            }
            sealer = new ExportSealer(request.publicKey().get());
            audit.add(Attr.RECIPIENT_KEY, sealer.fingerprint());
            audit.add(Attr.OUTCOME, "sealed");
        }
        if (request.fromOffset() != null && request.unprocessed())
        {
            throw new AdminArgs.UsageException(
                    "Use --from-offset or --unprocessed, not both");
        }
        Table landedItems = context.landedItems();
        landedItems.refresh();
        Record summary = LandedItems.summary(landedItems, itemId)
                .orElseThrow(() -> new IllegalStateException(
                        "No fully landed item with this id"));
        audit.add(Attr.TABLE, LandingTables.LANDED_ITEMS);
        long size = (Long) summary.getField(LandingTables.ITEM_SIZE);

        long from = request.unprocessed()
                ? unprocessedFrom(context.splitRecords(), itemId)
                : request.fromOffset() == null ? 0L : request.fromOffset();
        if (from > size)
        {
            throw new AdminArgs.UsageException("--from-offset is beyond the"
                    + " end of the item (" + size + " bytes)");
        }
        audit.add(Attr.OFFSET, from);

        String suffix = (from > 0 ? ".from-" + from : "")
                + (plaintext ? "" : ".jwe");
        String outputName = itemId + suffix;
        Optional<String> original = Optional.empty();
        if (request.originalName())
        {
            original = LandedItems.originalName(summary,
                    context.keys().wrap());
            if (original.isEmpty())
            {
                throw new IllegalStateException(
                        "Landing holds no original name for this item");
            }
            outputName = OutputFiles.safeName(original.get()) + suffix;
        }

        ExportSealer sealing = sealer;
        if (request.destination() == null)
        {
            long restored;
            try (Sealing sealed = new Sealing(invocation.sink()::sealed,
                    sealing, itemId, from, CALLER_PART_BYTES))
            {
                LandedItems.read(landedItems, itemId, context.keys().wrap(),
                        context.landingIds(), new Skipping(sealed, from));
                sealed.finish();
                restored = sealed.plaintext();
            }
            audit.add(Attr.BYTES, restored);
            return new Result(restored, from, size, false, "the caller");
        }
        long[] plain = new long[1];
        long written = request.destination().write(outputName, output ->
        {
            if (sealing == null)
            {
                LandedItems.read(landedItems, itemId, context.keys().wrap(),
                        context.landingIds(), new Skipping(output, from));
                return;
            }
            try (Sealing sealed = new Sealing(jwe -> output.write((jwe + "\n")
                    .getBytes(StandardCharsets.US_ASCII)), sealing, itemId,
                    from, PART_BYTES))
            {
                LandedItems.read(landedItems, itemId, context.keys().wrap(),
                        context.landingIds(), new Skipping(sealed, from));
                sealed.finish();
                plain[0] = sealed.plaintext();
            }
        });
        long restored = sealer == null ? written : plain[0];
        audit.add(Attr.BYTES, restored);
        // The original name is PHI, and stays out of output that may end
        // up in logs.
        return new Result(restored, from, size, plaintext,
                original.isPresent() ? request.destination().where(null)
                        + " under its original name"
                        : request.destination().where(outputName));
    }

    @Override
    public void print(Result result, PrintStream out)
    {
        out.printf("restored %d bytes (offset %d of %d) from landing,"
                + " digest verified, %s to %s%n", result.restored(),
                result.from(), result.size(),
                result.plaintext() ? "IN PLAINTEXT" : "sealed",
                result.where());
    }

    /**
     * Where an item's unprocessed tail starts: the end of its last split
     * record.
     *
     * @param records the stage's {@code split_records}
     * @param itemId  the item
     * @return the offset; 0 if no record of it was split
     * @throws IOException if the table can't be read
     */
    static long unprocessedFrom(Table records, UUID itemId)
            throws IOException
    {
        long end = 0;
        try (CloseableIterable<Record> rows = IcebergGenerics.read(records)
                .where(Expressions.and(
                        Expressions.equal(RecordsTable.ITEM_ID, itemId),
                        Expressions.equal(RecordsTable.CHUNK_INDEX, 0)))
                .select(RecordsTable.SOURCE_OFFSET,
                        RecordsTable.SOURCE_LENGTH)
                .build())
        {
            for (Record row : rows)
            {
                end = Math.max(end,
                        (Long) row.getField(RecordsTable.SOURCE_OFFSET)
                                + (Long) row.getField(
                                        RecordsTable.SOURCE_LENGTH));
            }
        }
        return end;
    }

    /**
     * Reads an item ID.
     *
     * @param text the argument
     * @return the ID
     * @throws AdminArgs.UsageException if it isn't a UUID
     */
    static UUID parseId(String text)
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
     * Reads an offset.
     *
     * @param text the argument
     * @return the offset
     * @throws AdminArgs.UsageException if it isn't a number of 0 or more
     */
    static long parseOffset(String text)
    {
        try
        {
            long value = Long.parseLong(text);
            if (value < 0)
            {
                throw new NumberFormatException();
            }
            return value;
        }
        catch (NumberFormatException notNumber)
        {
            throw new AdminArgs.UsageException(
                    "--from-offset must be a non-negative number");
        }
    }

    /** Where sealed parts go: lines of a file, or the API's caller. */
    @FunctionalInterface
    private interface Parts
    {

        /**
         * Takes a sealed part.
         *
         * @param jwe the part, a compact JWE
         * @throws IOException if it can't be written
         */
        void part(String jwe) throws IOException;
    }

    /**
     * Seals what is written to it in parts, each a JWE of its own,
     * numbered in its protected header with the item and where the part
     * starts, so parts can't be reordered, dropped or moved to another
     * item unseen. Each part's plaintext is cleared once sealed. Closing
     * it leaves what takes the parts open.
     */
    private static final class Sealing extends OutputStream
    {

        /** Where the sealed parts go. */
        private final Parts output;

        /** Seals the parts. */
        private final ExportSealer sealer;

        /** The item. */
        private final UUID itemId;

        /** The current part. */
        private final byte[] part;

        /** How much of the current part is filled. */
        private int filled;

        /** Where the current part starts, in the item. */
        private long partStart;

        /** Parts sealed so far. */
        private int parts;

        /** Plaintext bytes taken. */
        private long plaintext;

        /** Whether the last part has been sealed. */
        private boolean finished;

        /**
         * Starts sealing.
         *
         * @param output    where the sealed parts go
         * @param sealer    seals the parts
         * @param itemId    the item
         * @param from      where in the item the output starts
         * @param partBytes how much plaintext goes in a part
         */
        Sealing(Parts output, ExportSealer sealer, UUID itemId, long from,
                int partBytes)
        {
            this.output = output;
            this.part = new byte[partBytes];
            this.sealer = sealer;
            this.itemId = itemId;
            this.partStart = from;
        }

        @Override
        public void write(int value) throws IOException
        {
            write(new byte[] {(byte) value}, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                throws IOException
        {
            int at = offset;
            int left = length;
            while (left > 0)
            {
                int taken = Math.min(left, part.length - filled);
                System.arraycopy(bytes, at, part, filled, taken);
                filled += taken;
                at += taken;
                left -= taken;
                plaintext += taken;
                if (filled == part.length)
                {
                    seal(false);
                }
            }
        }

        /**
         * Seals what is left as the last part, which may be empty.
         *
         * @throws IOException if it can't be written
         */
        void finish() throws IOException
        {
            if (!finished)
            {
                seal(true);
                finished = true;
            }
        }

        /**
         * How much plaintext was sealed.
         *
         * @return the bytes
         */
        long plaintext()
        {
            return plaintext;
        }

        /**
         * Seals the current part and sends it on.
         *
         * @param last whether it is the last
         * @throws IOException if it can't be written
         */
        private void seal(boolean last) throws IOException
        {
            byte[] content = Arrays.copyOf(part, filled);
            Arrays.fill(part, 0, filled, (byte) 0);
            parts++;
            String line;
            try
            {
                line = sealer.seal(content, Map.of("sdh_item",
                        itemId.toString(), "sdh_part", parts, "sdh_offset",
                        partStart, "sdh_last", last));
            }
            finally
            {
                Arrays.fill(content, (byte) 0);
            }
            output.part(line);
            partStart += filled;
            filled = 0;
        }

        /** Clears what is unsealed; leaves the stream beneath open. */
        @Override
        public void close()
        {
            Arrays.fill(part, (byte) 0);
        }
    }

    /** Passes on only the bytes after an offset. */
    private static final class Skipping extends FilterOutputStream
    {

        /** Bytes still to drop. */
        private long skip;

        /**
         * Wraps a stream.
         *
         * @param output where the kept bytes go
         * @param skip   how many bytes to drop first
         */
        Skipping(OutputStream output, long skip)
        {
            super(output);
            this.skip = skip;
        }

        @Override
        public void write(int value) throws IOException
        {
            if (skip > 0)
            {
                skip--;
                return;
            }
            out.write(value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                throws IOException
        {
            int dropped = (int) Math.min(skip, length);
            skip -= dropped;
            if (length > dropped)
            {
                out.write(bytes, offset + dropped, length - dropped);
            }
        }
    }
}
