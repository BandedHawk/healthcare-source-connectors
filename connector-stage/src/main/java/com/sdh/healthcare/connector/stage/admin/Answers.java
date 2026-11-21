/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.sdh.healthcare.connector.core.checkpoint.LeaseStatus;
import com.sdh.healthcare.connector.stage.StageReleases;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * Builds the control API's answers ({@link Answering}): objects whose
 * values are text, numbers, booleans, nulls, lists and objects only.
 * Times are ISO-8601 instants and durations, IDs and enums their text.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class Answers
{

    /** Not instantiated. */
    private Answers()
    {
    }

    /**
     * An object from its fields, in order; a null value is kept as null.
     *
     * @param fields names and values, alternately
     * @return the object
     * @throws IllegalArgumentException if a name isn't text, or a name has
     *                                  no value
     */
    public static Map<String, Object> object(Object... fields)
    {
        if (fields.length % 2 != 0)
        {
            throw new IllegalArgumentException("A name without a value");
        }
        Map<String, Object> object = new LinkedHashMap<>();
        for (int index = 0; index < fields.length; index += 2)
        {
            if (!(fields[index] instanceof String name))
            {
                throw new IllegalArgumentException("A name must be text");
            }
            object.put(name, value(fields[index + 1]));
        }
        return object;
    }

    /**
     * A list of rows, each answered.
     *
     * @param rows   the rows
     * @param answer how one is answered
     * @param <T>    a row's type
     * @return the list
     */
    public static <T> List<Object> list(Collection<T> rows,
            Function<T, Object> answer)
    {
        List<Object> list = new ArrayList<>(rows.size());
        for (T row : rows)
        {
            list.add(answer.apply(row));
        }
        return list;
    }

    /**
     * A part read on its own: its answer, or that it was unavailable, by
     * the failure's class alone, never its message (HIPAA.md §4.1).
     *
     * @param part   the part; null if it wasn't asked for
     * @param answer how its value is answered
     * @param <T>    what it holds
     * @return the answer; null if it wasn't asked for
     */
    public static <T> Object part(Partial<T> part, Function<T, Object> answer)
    {
        if (part == null)
        {
            return null;
        }
        if (part.failure() != null)
        {
            return object("unavailable",
                    part.failure().getClass().getSimpleName());
        }
        return answer.apply(part.value());
    }

    /**
     * A lease, as inspected.
     *
     * @param lease the lease
     * @return its holder and expiry, or that it was never held
     */
    public static Map<String, Object> lease(StatusParts.Lease lease)
    {
        if (lease.status().isEmpty())
        {
            return object("name", lease.name(), "held", false, "everHeld",
                    false);
        }
        LeaseStatus status = lease.status().get();
        return object("name", lease.name(), "held", status.live(),
                "everHeld", true, "owner", status.owner(), "epoch",
                status.epoch(), "renewedAt", status.renewedAt(),
                "remaining", status.remaining());
    }

    /**
     * A release.
     *
     * @param release the release; may be null
     * @return its digest and versions; null for none
     */
    public static Map<String, Object> release(StageReleases.Release release)
    {
        return release == null ? null : object("digest", release.digest(),
                "versions", release.versions());
    }

    /**
     * A release let in.
     *
     * @param letIn the release and how it was let in; may be null
     * @return it; null for none
     */
    public static Map<String, Object> letIn(StageReleases.LetIn letIn)
    {
        return letIn == null ? null : object("release",
                release(letIn.release()), "how", letIn.how(), "at",
                letIn.at());
    }

    /**
     * A value as the API answers it.
     *
     * @param value the value
     * @return it, as text where it isn't a number, boolean, list or map;
     *         a list's values each answered
     */
    private static Object value(Object value)
    {
        if (value == null || value instanceof String
                || value instanceof Number || value instanceof Boolean
                || value instanceof Map)
        {
            return value;
        }
        if (value instanceof Instant || value instanceof Duration
                || value instanceof UUID || value instanceof Enum
                || value instanceof TemporalAccessor)
        {
            return value.toString();
        }
        if (value instanceof Collection<?> collection)
        {
            List<Object> list = new ArrayList<>(collection.size());
            for (Object each : collection)
            {
                list.add(value(each));
            }
            return list;
        }
        throw new IllegalArgumentException("Not an answer's value: "
                + value.getClass().getSimpleName());
    }
}
