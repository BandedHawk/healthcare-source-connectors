/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsPrefixOperations;

/**
 * A scope's control changes, kept in the object store beside its tables
 * (DESIGN.md §18.6): one small object for each purge, erasure, rotation,
 * re-key and retention change, written once the change is made in the
 * database. A restore takes the database back in time, but
 * not the store, so these say what was changed since the restore's point,
 * and its check makes the purges and erasures again, so purged records
 * stay purged.
 * <p>
 * Each holds the change's operation ID, kind, scope, time and principal,
 * and its own fields: an item ID, a ticket, a KEK reference, a duration.
 * Never a document's value or a reason's text beyond what
 * {@link ControlState#REASON} allows.
 * <p>
 * They are kept under {@code <namespace>/_control/<scope>/}, the namespace
 * being the directory of the stage's tables, so dropping a table (an
 * erase) leaves them.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ControlLog
{

    /** The directory beside the tables. */
    public static final String DIRECTORY = "_control";

    /** A field's name. */
    private static final Pattern FIELD = Pattern.compile("[a-z][A-Za-z]{0,30}");

    /** The kinds of change. */
    public enum Kind
    {
        /** An item purged. */
        PURGE,
        /** The stage erased. */
        ERASE,
        /** Its KEK rotated. */
        ROTATE,
        /** What it stores re-keyed. */
        REKEY,
        /** Its retention set. */
        RETENTION;

        /**
         * The kind's name in an entry.
         *
         * @return it, in lower case
         */
        public String wire()
        {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /**
     * One change.
     *
     * @param id     its operation's ID
     * @param kind   what kind
     * @param scope  its scope
     * @param at     when it was made
     * @param by     who made it
     * @param fields its own fields
     */
    public record Entry(String id, Kind kind, String scope, Instant at,
            String by, Map<String, String> fields)
    {

        /**
         * Checks the parts, and copies the fields.
         */
        public Entry
        {
            Objects.requireNonNull(id, "id");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(at, "at");
            Objects.requireNonNull(by, "by");
            fields = Map.copyOf(fields);
            fields.keySet().forEach(name ->
            {
                if (!FIELD.matcher(name).matches())
                {
                    throw new IllegalArgumentException("A field's name must"
                            + " be letters");
                }
            });
        }

        /**
         * A field.
         *
         * @param name its name
         * @return its value; null if there is none
         */
        public String field(String name)
        {
            return fields.get(name);
        }
    }

    /** The store. */
    private final FileIO io;

    /** The scope's directory, ending in a slash. */
    private final String directory;

    /** The scope. */
    private final String scope;

    /**
     * Opens a scope's log.
     *
     * @param io        the store
     * @param namespace the directory the stage's tables are in
     * @param scope     the scope
     */
    public ControlLog(FileIO io, String namespace, String scope)
    {
        this.io = Objects.requireNonNull(io, "io");
        this.scope = Objects.requireNonNull(scope, "scope");
        String base = namespace.endsWith("/") ? namespace : namespace + "/";
        this.directory = base + DIRECTORY + "/" + Base64.getUrlEncoder()
                .withoutPadding().encodeToString(scope.getBytes(
                        StandardCharsets.UTF_8)) + "/";
    }

    /**
     * The directory a table's location is in: its namespace's.
     *
     * @param location the table's location
     * @return the directory
     */
    public static String namespaceOf(String location)
    {
        String trimmed = location.endsWith("/")
                ? location.substring(0, location.length() - 1) : location;
        int slash = trimmed.lastIndexOf('/');
        if (slash < 0 || trimmed.indexOf("://") == slash - 2)
        {
            throw new IllegalArgumentException("A table's location must be"
                    + " under a directory");
        }
        return trimmed.substring(0, slash);
    }

    /**
     * The scope.
     *
     * @return it
     */
    public String scope()
    {
        return scope;
    }

    /**
     * Keeps a change. An entry of the same operation and kind already kept
     * is left as it is.
     *
     * @param kind   what kind
     * @param id     its operation's ID
     * @param by     who made it
     * @param fields its own fields
     * @return the entry
     * @throws UncheckedIOException if it can't be written
     */
    public Entry keep(Kind kind, String id, String by,
            Map<String, String> fields)
    {
        Entry entry = new Entry(id, kind, scope, Instant.now(), by, fields);
        String location = directory + kind.wire() + "-" + safe(id) + ".json";
        if (exists(location))
        {
            return entry;
        }
        Map<String, Object> json = new LinkedHashMap<>();
        json.put("id", entry.id());
        json.put("kind", kind.wire());
        json.put("scope", scope);
        json.put("at", entry.at().toString());
        json.put("by", entry.by());
        json.put("fields", new LinkedHashMap<>(entry.fields()));
        OutputFile file = io.newOutputFile(location);
        try (OutputStream out = file.createOrOverwrite())
        {
            out.write(JSONObjectUtils.toJSONString(json)
                    .getBytes(StandardCharsets.UTF_8));
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException("Could not keep the control"
                    + " change in the store", failure);
        }
        return entry;
    }

    /**
     * Every change kept, oldest first.
     *
     * @return them
     * @throws IllegalStateException if the store can't list, or an entry
     *                               can't be read
     */
    public List<Entry> entries()
    {
        if (!(io instanceof SupportsPrefixOperations listing))
        {
            throw new IllegalStateException("The store can't list, so the"
                    + " control changes can't be read");
        }
        List<Entry> entries = new ArrayList<>();
        for (FileInfo file : listing.listPrefix(directory))
        {
            String location = file.location().contains("://")
                    ? file.location()
                    : directory + file.location().substring(
                            file.location().lastIndexOf('/') + 1);
            if (location.endsWith(".json"))
            {
                entries.add(read(location));
            }
        }
        entries.sort(Comparator.comparing(Entry::at)
                .thenComparing(Entry::id));
        return entries;
    }

    /**
     * Reads an entry.
     *
     * @param location its object
     * @return it
     * @throws IllegalStateException if it can't be read
     */
    private Entry read(String location)
    {
        try (InputStream in = io.newInputFile(location).newStream())
        {
            Map<String, Object> json = JSONObjectUtils.parse(new String(
                    in.readAllBytes(), StandardCharsets.UTF_8));
            Map<String, String> fields = new LinkedHashMap<>();
            Map<String, Object> given = JSONObjectUtils.getJSONObject(json,
                    "fields");
            if (given != null)
            {
                given.forEach((name, value) -> fields.put(name,
                        value == null ? null : value.toString()));
            }
            fields.values().removeIf(Objects::isNull);
            return new Entry(JSONObjectUtils.getString(json, "id"),
                    Kind.valueOf(JSONObjectUtils.getString(json, "kind")
                            .toUpperCase(Locale.ROOT)),
                    JSONObjectUtils.getString(json, "scope"),
                    Instant.parse(JSONObjectUtils.getString(json, "at")),
                    JSONObjectUtils.getString(json, "by"), fields);
        }
        catch (IOException | ParseException | RuntimeException failure)
        {
            throw new IllegalStateException("A control change kept in the"
                    + " store can't be read", failure);
        }
    }

    /**
     * Whether an object is there. Some stores answer a missing one by
     * failing to open it rather than saying it isn't there.
     *
     * @param location the object
     * @return true if it is
     */
    private boolean exists(String location)
    {
        try
        {
            return io.newInputFile(location).exists();
        }
        catch (NotFoundException missing)
        {
            return false;
        }
    }

    /**
     * An operation ID safe in an object's name.
     *
     * @param id the ID
     * @return it, or a UUID made from it if it isn't plain
     */
    private static String safe(String id)
    {
        return id.matches("[A-Za-z0-9._-]{1,80}") ? id
                : UUID.nameUUIDFromBytes(id.getBytes(StandardCharsets.UTF_8))
                        .toString();
    }
}
