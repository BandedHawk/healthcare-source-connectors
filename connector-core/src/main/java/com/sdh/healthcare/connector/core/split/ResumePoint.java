package com.sdh.healthcare.connector.core.split;

import java.util.ArrayList;
import java.util.List;

/**
 * Where a split session continues after a restart (design §6.1, §9.1).
 *
 * <p>{@code offset} is where scanning resumes. {@code context} holds the source offsets of any earlier
 * structures the splitter must re-read to carry on, such as an X12 envelope or the open XML ancestors.
 * It holds offsets only, never content, so it is PHI-free and can be stored in the checkpoint.
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
