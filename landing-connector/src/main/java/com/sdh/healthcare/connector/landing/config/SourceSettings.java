/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Locale;

/**
 * A directory source's settings, checked and turned into types (design
 * §5.3.4). Bad settings stop the node at startup, with a message that names
 * the setting.
 *
 * @param path         the directory, made absolute
 * @param access       whether the node may change the directory
 * @param after        what happens to an item after landing
 * @param moveTo       where {@link After#MOVE} puts items, made absolute;
 *                     null otherwise
 * @param renameSuffix what {@link After#RENAME} appends
 * @param marker       whether an item is ready once its {@code .done}
 *                     marker is there, rather than once it has settled
 * @param settle       how long an item must stay unchanged; zero with a
 *                     marker
 * @param include      an Ant-style pattern of the paths to pick up
 * @param recursive    whether to look in subdirectories
 * @param pollInterval how often the directory is listed
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SourceSettings(Path path, Access access, After after,
        Path moveTo, String renameSuffix, boolean marker, Duration settle,
        String include, boolean recursive, Duration pollInterval)
{

    /** What a marker file's name adds to its item's name. */
    public static final String MARKER_SUFFIX = ".done";

    /** Whether the node may change the source. */
    public enum Access
    {
        /** Nothing in the source is changed; the ledger alone stops repeats. */
        READ_ONLY,

        /** Each item is deleted, moved or renamed after landing. */
        READ_WRITE
    }

    /** What happens to an item after it is landed and announced. */
    public enum After
    {
        /** It is left where it is: the only choice on a read-only source. */
        NONE,

        /** It is deleted. */
        DELETE,

        /** It is moved to another directory, keeping its relative path. */
        MOVE,

        /** A suffix is added to its name. */
        RENAME
    }

    /**
     * Checks a source's settings.
     *
     * @param source the settings as bound
     * @return the checked settings
     * @throws IllegalArgumentException if a setting is missing, unknown,
     *                                  or doesn't fit with the others
     */
    public static SourceSettings of(LandingProperties.Source source)
    {
        if (source.path() == null)
        {
            throw new IllegalArgumentException(
                    "sdh.landing.source.path is required");
        }
        Path path = source.path().toAbsolutePath().normalize();
        Access access = parse(Access.class, source.access(),
                "sdh.landing.source.access");
        After after = source.after() == null
                ? (access == Access.READ_ONLY ? After.NONE : After.DELETE)
                : parse(After.class, source.after(),
                        "sdh.landing.source.after");
        if (access == Access.READ_ONLY && after != After.NONE)
        {
            throw new IllegalArgumentException("sdh.landing.source.after"
                    + " must be none on a read-only source");
        }
        if (access == Access.READ_WRITE && after == After.NONE)
        {
            throw new IllegalArgumentException("sdh.landing.source.after"
                    + " must be delete, move or rename on a read-write"
                    + " source");
        }
        Path moveTo = null;
        if (after == After.MOVE)
        {
            if (source.moveTo() == null)
            {
                throw new IllegalArgumentException(
                        "sdh.landing.source.move-to is required with move");
            }
            moveTo = source.moveTo().toAbsolutePath().normalize();
            if (moveTo.equals(path))
            {
                throw new IllegalArgumentException(
                        "sdh.landing.source.move-to must not be the source");
            }
        }
        if (after == After.RENAME && (source.renameSuffix() == null
                || source.renameSuffix().isBlank()))
        {
            throw new IllegalArgumentException(
                    "sdh.landing.source.rename-suffix is required with"
                            + " rename");
        }
        boolean marker = switch (source.ready())
        {
            case "settled" -> false;
            case "marker" -> true;
            default -> throw new IllegalArgumentException(
                    "sdh.landing.source.ready must be settled or marker");
        };
        if (!marker && source.settle().isNegative())
        {
            throw new IllegalArgumentException(
                    "sdh.landing.source.settle must not be negative");
        }
        if (source.pollInterval().isZero()
                || source.pollInterval().isNegative())
        {
            throw new IllegalArgumentException(
                    "sdh.landing.source.poll-interval must be positive");
        }
        return new SourceSettings(path, access, after, moveTo,
                source.renameSuffix(), marker,
                marker ? Duration.ZERO : source.settle(), source.include(),
                source.recursive(), source.pollInterval());
    }

    /**
     * Whether the node may change the source.
     *
     * @return true for a read-write source
     */
    public boolean writable()
    {
        return access == Access.READ_WRITE;
    }

    /**
     * Turns a setting such as {@code read-only} into its constant.
     *
     * @param <E>     the enum
     * @param type    the enum's class
     * @param value   the setting's value
     * @param setting the setting's name, for the message
     * @return the constant
     * @throws IllegalArgumentException if the value isn't one of them
     */
    private static <E extends Enum<E>> E parse(Class<E> type, String value,
            String setting)
    {
        String name = value == null ? ""
                : value.trim().replace('-', '_').toUpperCase(Locale.ROOT);
        for (E constant : type.getEnumConstants())
        {
            if (constant.name().equals(name))
            {
                return constant;
            }
        }
        throw new IllegalArgumentException(setting + " has an unknown value");
    }
}
