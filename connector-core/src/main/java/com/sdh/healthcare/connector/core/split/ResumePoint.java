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
 *
 * <p>{@code offset} is where scanning resumes. {@code context} holds the source offsets of any earlier
 * structures the splitter must re-read to carry on, such as an X12 envelope or the open XML ancestors.
 * It holds offsets only, never content, so it is PHI-free and can be stored in the checkpoint.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ResumePoint(long offset, List<Long> context) {
    public static final ResumePoint START = new ResumePoint(0, List.of());

    public ResumePoint {
        if (offset < 0) {
            throw new IllegalArgumentException("Negative offset");
        }
        context = List.copyOf(context);
    }

    public static ResumePoint at(long offset, long... context) {
        List<Long> c = new ArrayList<>(context.length);
        for (long v : context) {
            c.add(v);
        }
        return new ResumePoint(offset, c);
    }

    /**
     * Whether this point is past another: a greater offset, or the same
     * offset with a greater context, compared entry by entry, a longer
     * context being past a shorter one it starts with. A splitter whose
     * sections share an offset, such as the rows of a Parquet row group,
     * counts on in its context.
     *
     * @param other the other point
     * @return true if this point is past it
     */
    public boolean isAfter(ResumePoint other) {
        if (offset != other.offset) {
            return offset > other.offset;
        }
        int common = Math.min(context.size(), other.context.size());
        for (int i = 0; i < common; i++) {
            int order = Long.compare(context.get(i), other.context.get(i));
            if (order != 0) {
                return order > 0;
            }
        }
        return context.size() > other.context.size();
    }

    /** Compact text form for the checkpoint: {@code "<offset>"} or {@code "<offset>:<c0>,<c1>,..."}. */
    public String encode() {
        if (context.isEmpty()) {
            return Long.toString(offset);
        }
        StringBuilder sb = new StringBuilder().append(offset).append(':');
        for (int i = 0; i < context.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(context.get(i));
        }
        return sb.toString();
    }

    public static ResumePoint parse(String s) {
        int colon = s.indexOf(':');
        if (colon < 0) {
            return new ResumePoint(Long.parseLong(s), List.of());
        }
        List<Long> c = new ArrayList<>();
        for (String part : s.substring(colon + 1).split(",")) {
            c.add(Long.parseLong(part));
        }
        return new ResumePoint(Long.parseLong(s.substring(0, colon)), c);
    }
}
