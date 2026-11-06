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
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
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
final class RestoreCommand implements AdminCommand
{

    /** The command's help. */
    static final String USAGE = """
            restore --file-id <id> --to <dir>
                    [--from-offset <n> | --unprocessed] [--original-name]
                Decrypts a landed item into <dir>, checking its digest.
                --from-offset   start at this byte offset instead of 0
                --unprocessed   start after the item's last stored record:
                                only its unprocessed tail (headers and
                                envelopes before that point aren't repeated)
                --original-name name the output after the original file
                                instead of <id>""";

    /** Creates the command. */
    RestoreCommand()
    {
    }

    @Override
    public String name()
    {
        return "restore";
    }

    @Override
    public Set<String> flags()
    {
        return Set.of("unprocessed", "original-name");
    }

    @Override
    public Set<String> options()
    {
        return Set.of("file-id", "to", "from-offset", "unprocessed",
                "original-name");
    }

    @Override
    public void run(AdminArgs arguments, AdminContext context,
            AuditRecord audit, PrintStream out) throws Exception
    {
        UUID itemId = parseId(arguments.required("file-id"));
        audit.add(Attr.ITEM_ID, itemId);
        Path directory = OutputFiles.directory(arguments.required("to"));
        if (arguments.option("from-offset").isPresent()
                && arguments.flag("unprocessed"))
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

        long from = arguments.flag("unprocessed")
                ? unprocessedFrom(context.records(), itemId)
                : arguments.option("from-offset")
                        .map(RestoreCommand::parseOffset).orElse(0L);
        if (from > size)
        {
            throw new AdminArgs.UsageException("--from-offset is beyond the"
                    + " end of the item (" + size + " bytes)");
        }
        audit.add(Attr.OFFSET, from);

        String suffix = from > 0 ? ".from-" + from : "";
        String outputName = itemId + suffix;
        Optional<String> original = Optional.empty();
        if (arguments.flag("original-name"))
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

        long written = OutputFiles.writeNew(directory, outputName,
                output -> LandedItems.read(landedItems, itemId,
                        context.keys().wrap(), context.landingIds(),
                        new Skipping(output, from)));
        audit.add(Attr.BYTES, written);
        // The original name is PHI, and stays out of output that may end
        // up in logs.
        out.printf("restored %d bytes (offset %d of %d) from landing,"
                + " digest verified, to %s%n", written, from, size,
                original.isPresent()
                        ? directory + " under its original name"
                        : directory.resolve(outputName));
    }

    /**
     * Where an item's unprocessed tail starts: the end of its last stored
     * record.
     *
     * @param records the {@code records} table
     * @param itemId  the item
     * @return the offset; 0 if no record of it is stored
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
            throw new AdminArgs.UsageException("--file-id must be a UUID");
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
