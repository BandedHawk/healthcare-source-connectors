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
 * A source's settings, checked and turned into types (design §5.3.1,
 * §5.3.4, §5.3.17). Bad settings stop the node at startup, with a message
 * that names the setting. For a remote source, paths are on the server
 * (within the share, for SMB; key prefixes in the bucket, for an object
 * store) and must be absolute.
 *
 * @param path         the directory, made absolute; null for
 *                     {@link Type#HTTP} and {@link Type#QUEUE}
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
 * @param pollInterval how often the source is listed
 * @param type         what kind of source it is
 * @param authRetryInterval how long a remote source waits after its
 *                     credentials, permissions, host key or share were
 *                     refused
 * @param sftp         the server, for {@link Type#SFTP}; null otherwise
 * @param smb          the share, for {@link Type#SMB}; null otherwise
 * @param bucket       the bucket, for {@link Type#S3},
 *                     {@link Type#AZURE_BLOB} and {@link Type#GCS}; null
 *                     otherwise
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record SourceSettings(Path path, Access access, After after,
        Path moveTo, String renameSuffix, boolean marker, Duration settle,
        String include, boolean recursive, Duration pollInterval,
        Type type, Duration authRetryInterval, SftpSettings sftp,
        SmbSettings smb, BucketSettings bucket)
{

    /** What a marker file's name adds to its item's name. */
    public static final String MARKER_SUFFIX = ".done";

    /** What kind of source it is. */
    public enum Type
    {

        /** A local or mounted directory, listed by Camel's file component. */
        DIRECTORY,

        /** A directory on an SFTP server. */
        SFTP,

        /** A directory on an SMB share. */
        SMB,

        /** A prefix in an S3 bucket, or one on an S3-compatible store. */
        S3,

        /** A prefix in an Azure Blob Storage container. */
        AZURE_BLOB,

        /** A prefix in a Google Cloud Storage bucket. */
        GCS,

        /**
         * Items pushed to the node over HTTP; its endpoint has its own
         * settings ({@link HttpSettings}), and none of the listing ones
         * apply.
         */
        HTTP,

        /**
         * Messages consumed from a broker in batches; its broker has its
         * own settings ({@link BrokerSettings}), and none of the listing
         * ones apply.
         */
        QUEUE;

        /**
         * Whether this is an object store.
         *
         * @return true for S3, Azure Blob and GCS
         */
        public boolean bucket()
        {
            return this == S3 || this == AZURE_BLOB || this == GCS;
        }
    }

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
        Type type = parse(Type.class, source.type(),
                "sdh.landing.source.type");
        if (type == Type.HTTP || type == Type.QUEUE)
        {
            return new SourceSettings(null, Access.READ_ONLY, After.NONE,
                    null, null, false, Duration.ZERO, null, false,
                    source.pollInterval(), type, null, null, null, null);
        }
        if (source.path() == null)
        {
            throw new IllegalArgumentException(
                    "sdh.landing.source.path is required");
        }
        Path path = place(source.path(), type, "sdh.landing.source.path");
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
            moveTo = place(source.moveTo(), type,
                    "sdh.landing.source.move-to");
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
        if (type != Type.DIRECTORY && (source.authRetryInterval() == null
                || source.authRetryInterval().isNegative()
                || source.authRetryInterval().isZero()))
        {
            throw new IllegalArgumentException(
                    "sdh.landing.source.auth-retry-interval must be positive");
        }
        return new SourceSettings(path, access, after, moveTo,
                source.renameSuffix(), marker,
                marker ? Duration.ZERO : source.settle(), source.include(),
                source.recursive(), source.pollInterval(), type,
                source.authRetryInterval(),
                type == Type.SFTP ? SftpSettings.of(source.sftp()) : null,
                type == Type.SMB ? SmbSettings.of(source.smb()) : null,
                type.bucket() ? BucketSettings.of(type, source.bucket())
                        : null);
    }

    /**
     * Whether the source is on a server rather than a local or mounted
     * directory.
     *
     * @return true for SFTP, SMB and the object stores
     */
    public boolean remote()
    {
        return type == Type.SFTP || type == Type.SMB || type.bucket();
    }

    /**
     * Normalises a path setting: a local one is made absolute; a remote
     * one must already be, since there is no working directory to resolve
     * it against.
     *
     * @param value   the setting
     * @param type    the source's type
     * @param setting its name, for the message
     * @return the normalised path
     * @throws IllegalArgumentException if a remote path is relative
     */
    private static Path place(Path value, Type type, String setting)
    {
        if (type == Type.DIRECTORY)
        {
            return value.toAbsolutePath().normalize();
        }
        if (!value.isAbsolute())
        {
            throw new IllegalArgumentException(setting + " must be absolute"
                    + " on a remote source (/ is the share's root on SMB,"
                    + " the bucket's on an object store)");
        }
        return value.normalize();
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
