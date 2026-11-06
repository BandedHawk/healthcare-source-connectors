/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split;

import java.util.ArrayList;
import java.util.List;

/**
 * Where a split session continues after a restart (design §6.1, §9.1).
 * <p>
 * {@code offset} is where scanning resumes. {@code context} holds the
 * source offsets of any earlier structures the splitter must re-read to
 * carry on, such as an X12 envelope or the open XML ancestors. It holds
 * offsets only, never content, so it is PHI-free and can be stored in the
 * checkpoint.
 *
 * @param offset  where scanning resumes
 * @param context offsets of earlier structures to re-read
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ResumePoint(long offset, List<Long> context)
{

    /** The start of the file. */
    public static final ResumePoint START = new ResumePoint(0, List.of());

    /**
     * Checks the offset and copies the context.
     *
     * @param offset  where scanning resumes
     * @param context offsets of earlier structures
     * @throws IllegalArgumentException if the offset is negative
     */
    public ResumePoint
    {
        if (offset < 0)
        {
            throw new IllegalArgumentException("Negative offset");
        }
        context = List.copyOf(context);
    }

    /**
     * A resume point.
     *
     * @param offset  where scanning resumes
     * @param context offsets of earlier structures
     * @return the point
     */
    public static ResumePoint at(long offset, long... context)
    {
        List<Long> entries = new ArrayList<>(context.length);
        for (long value : context)
        {
            entries.add(value);
        }
        return new ResumePoint(offset, entries);
    }

    /**
     * Whether this point is past another: a greater offset, or the same
     * offset with a greater context, compared entry by entry, a longer
     * context being past a shorter one it starts with. A splitter whose
     * records share an offset, such as the rows of a Parquet row group,
     * counts on in its context.
     *
     * @param other the other point
     * @return true if this point is past it
     */
    public boolean isAfter(ResumePoint other)
    {
        if (offset != other.offset)
        {
            return offset > other.offset;
        }
        int common = Math.min(context.size(), other.context.size());
        for (int index = 0; index < common; index++)
        {
            int order = Long.compare(context.get(index),
                    other.context.get(index));
            if (order != 0)
            {
                return order > 0;
            }
        }
        return context.size() > other.context.size();
    }

    /**
     * Compact text form for the checkpoint: {@code "<offset>"} or
     * {@code "<offset>:<c0>,<c1>,..."}.
     *
     * @return the text
     */
    public String encode()
    {
        if (context.isEmpty())
        {
            return Long.toString(offset);
        }
        StringBuilder text = new StringBuilder().append(offset).append(':');
        for (int index = 0; index < context.size(); index++)
        {
            if (index > 0)
            {
                text.append(',');
            }
            text.append(context.get(index));
        }
        return text.toString();
    }

    /**
     * Reads the text form {@link #encode()} writes.
     *
     * @param text the text
     * @return the point
     * @throws NumberFormatException    if a number is malformed
     * @throws IllegalArgumentException if the offset is negative
     */
    public static ResumePoint parse(String text)
    {
        int colon = text.indexOf(':');
        if (colon < 0)
        {
            return new ResumePoint(Long.parseLong(text), List.of());
        }
        List<Long> entries = new ArrayList<>();
        for (String part : text.substring(colon + 1).split(","))
        {
            entries.add(Long.parseLong(part));
        }
        return new ResumePoint(Long.parseLong(text.substring(0, colon)),
                entries);
    }
}
