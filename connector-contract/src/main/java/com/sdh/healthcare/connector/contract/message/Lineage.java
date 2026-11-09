/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.message;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * Where an object stands in its line of processing (STAGES.md §15.8.2): the
 * generation of its item, which a reprocess at the split stage raises, and
 * a revision per stage it has passed, in pipeline order, which a reprocess,
 * retry or correction of one record at that stage raises. Written out as,
 * for example, {@code g2 r1.3.1}: generation 2, the split stage's first cut
 * of it, the convert stage's third revision, the transform stage's first.
 * <p>
 * A stage not yet passed, or passed once, counts as revision 1, so trailing
 * 1s are left out: {@code r1.3.1} is kept as {@code [1, 3]}, and an object
 * processed once everywhere has an empty line. Every object made before
 * lineage was kept is {@link #FIRST}, so its message IDs are unchanged.
 * <p>
 * Of two objects of one item, the one with the higher generation is
 * current, whatever their places; within a generation, of two at the same
 * place, the one whose line is higher, compared stage by stage from the
 * left ({@link #compareTo}). Revisions of two places, or of two generations,
 * are never compared.
 *
 * @param generation the item's generation, from 1
 * @param line       a revision per stage, from 1, without trailing 1s
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Lineage(int generation, List<Integer> line)
        implements Comparable<Lineage>
{

    /** The split stage's place in a line. */
    public static final int SPLIT = 0;

    /** The convert stage's place in a line. */
    public static final int CONVERT = 1;

    /** The transform stage's place in a line. */
    public static final int TRANSFORM = 2;

    /** An object processed once at every stage, in the first generation. */
    public static final Lineage FIRST = new Lineage(1, List.of());

    /**
     * Checks the numbers, and leaves out trailing 1s.
     *
     * @throws IllegalArgumentException if a number is below 1
     */
    public Lineage
    {
        if (generation < 1)
        {
            throw new IllegalArgumentException("A generation is from 1");
        }
        List<Integer> kept = new ArrayList<>(line == null ? List.of() : line);
        for (Integer revision : kept)
        {
            if (revision == null || revision < 1)
            {
                throw new IllegalArgumentException("A revision is from 1");
            }
        }
        while (!kept.isEmpty() && kept.get(kept.size() - 1) == 1)
        {
            kept.remove(kept.size() - 1);
        }
        line = List.copyOf(kept);
    }

    /**
     * A lineage, with nothing given read as the first.
     *
     * @param generation the generation; 0 or less for the first
     * @param line       the revisions; null for none
     * @return the lineage
     */
    public static Lineage of(int generation, List<Integer> line)
    {
        return new Lineage(Math.max(1, generation), line);
    }

    /**
     * A lineage, or the first if none is given.
     *
     * @param lineage the lineage, or null
     * @return it, or {@link #FIRST}
     */
    public static Lineage orFirst(Lineage lineage)
    {
        return lineage == null ? FIRST : lineage;
    }

    /**
     * Whether this is the first of everything: generation 1, revision 1 at
     * every stage.
     *
     * @return true if it is
     */
    public boolean first()
    {
        return generation == 1 && line.isEmpty();
    }

    /**
     * A stage's revision.
     *
     * @param stage the stage's place, such as {@link #CONVERT}
     * @return its revision, from 1
     */
    public int revision(int stage)
    {
        return stage < line.size() ? line.get(stage) : 1;
    }

    /**
     * The same lineage with a stage's revision set: what that stage's
     * output carries.
     *
     * @param stage    the stage's place
     * @param revision its revision, from 1
     * @return the lineage
     */
    public Lineage withRevision(int stage, int revision)
    {
        List<Integer> revised = new ArrayList<>(line);
        while (revised.size() <= stage)
        {
            revised.add(1);
        }
        revised.set(stage, revision);
        return new Lineage(generation, revised);
    }

    /**
     * Only the revisions of the stages up to one: what an input of the
     * stage after it carries, without any later stage's.
     *
     * @param stage the last stage kept
     * @return the lineage
     */
    public Lineage upTo(int stage)
    {
        return new Lineage(generation,
                line.subList(0, Math.min(line.size(), stage + 1)));
    }

    /**
     * Compares two lineages of one item: by generation, then by revision
     * stage by stage from the left, a stage not given counting as 1.
     *
     * @param other the other
     * @return below 0 if this is older, above 0 if newer, 0 if the same
     */
    @Override
    public int compareTo(Lineage other)
    {
        int byGeneration = Integer.compare(generation, other.generation);
        if (byGeneration != 0)
        {
            return byGeneration;
        }
        int length = Math.max(line.size(), other.line.size());
        for (int stage = 0; stage < length; stage++)
        {
            int byRevision = Integer.compare(revision(stage),
                    other.revision(stage));
            if (byRevision != 0)
            {
                return byRevision;
            }
        }
        return 0;
    }

    /**
     * What a message ID, or a registry key, gains for this lineage:
     * {@code ~g<n>} past generation 1 and {@code ~r<line>} past revision 1,
     * so the first of everything adds nothing, and IDs from before lineage
     * was kept stay the same.
     *
     * @return the suffix, or nothing
     */
    public String suffix()
    {
        return (generation > 1 ? "~g" + generation : "")
                + (line.isEmpty() ? "" : "~r" + lineText());
    }

    /**
     * The revisions, joined with dots, such as {@code 1.3}; empty for the
     * first at every stage.
     *
     * @return the text
     */
    public String lineText()
    {
        StringJoiner joined = new StringJoiner(".");
        for (Integer revision : line)
        {
            joined.add(revision.toString());
        }
        return joined.toString();
    }

    /**
     * Reads revisions joined with dots, as {@link #lineText} writes them.
     *
     * @param text the text; null or empty for none
     * @return the revisions
     * @throws IllegalArgumentException if it isn't numbers joined by dots
     */
    public static List<Integer> parseLine(String text)
    {
        List<Integer> line = new ArrayList<>();
        if (text == null || text.isEmpty())
        {
            return line;
        }
        for (String part : text.split("\\.", -1))
        {
            line.add(Integer.valueOf(part));
        }
        return line;
    }

    /**
     * The lineage as telemetry and operators see it, such as
     * {@code g2 r1.3}.
     *
     * @return the text
     */
    @Override
    public String toString()
    {
        return "g" + generation + " r" + (line.isEmpty() ? "1" : lineText());
    }
}
