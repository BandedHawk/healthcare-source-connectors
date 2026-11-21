/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.audit;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

/**
 * One audit event in a chain (HIPAA.md §3.1, audit controls): its place,
 * and the hash that links it to the event before. An entry's hash is
 * SHA-256 over the previous entry's hash and the entry's every field, each
 * prefixed with its length, so changing, removing, reordering or inserting
 * an entry changes every hash after it. The first entry of a chain follows
 * {@link #GENESIS}.
 *
 * @param chainId    the chain: one process's events
 * @param seq        the entry's number in the chain, from 1
 * @param at         when the event was emitted, to the microsecond
 * @param name       the event's name, such as {@code sdh.audit.item.landed}
 * @param severity   {@code INFO}, {@code WARN} or {@code ERROR}
 * @param attributes its allow-listed attributes, by key, as text
 * @param hash       the entry's hash
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record AuditEntry(UUID chainId, long seq, Instant at, String name,
        String severity, SortedMap<String, String> attributes, byte[] hash)
{

    /** What the first entry of a chain follows: 32 zero bytes. */
    static final byte[] GENESIS = new byte[32];

    /**
     * Copies the attributes and hash, so the entry can't be changed.
     */
    public AuditEntry
    {
        attributes = new TreeMap<>(attributes);
        hash = hash.clone();
    }

    /**
     * The entry's hash.
     *
     * @return a copy of it
     */
    @Override
    public byte[] hash()
    {
        return hash.clone();
    }

    /**
     * The entry's attributes.
     *
     * @return a copy of them
     */
    @Override
    public SortedMap<String, String> attributes()
    {
        return new TreeMap<>(attributes);
    }

    /**
     * Makes the entry after another, or the first of a chain.
     *
     * @param chainId    the chain
     * @param seq        the entry's number, from 1
     * @param at         when the event was emitted; kept to the
     *                   microsecond, as the database keeps it
     * @param name       the event's name
     * @param severity   its severity
     * @param attributes its attributes
     * @param previous   the hash of the entry before; {@link #GENESIS}
     *                   for the first
     * @return the entry
     */
    public static AuditEntry next(UUID chainId, long seq, Instant at,
            String name, String severity,
            SortedMap<String, String> attributes, byte[] previous)
    {
        Instant micros = at.truncatedTo(ChronoUnit.MICROS);
        return new AuditEntry(chainId, seq, micros, name, severity,
                attributes, hash(previous, chainId, seq, micros, name,
                        severity, attributes));
    }

    /**
     * Computes an entry's hash.
     *
     * @param previous   the hash of the entry before
     * @param chainId    the chain
     * @param seq        the entry's number
     * @param at         when, to the microsecond
     * @param name       the event's name
     * @param severity   its severity
     * @param attributes its attributes
     * @return the SHA-256 hash
     */
    public static byte[] hash(byte[] previous, UUID chainId, long seq,
            Instant at, String name, String severity,
            SortedMap<String, String> attributes)
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException missing)
        {
            throw new IllegalStateException(missing);
        }
        digest.update(previous);
        field(digest, chainId.toString());
        field(digest, Long.toString(seq));
        field(digest, Long.toString(micros(at)));
        field(digest, name);
        field(digest, severity);
        field(digest, Integer.toString(attributes.size()));
        attributes.forEach((key, value) ->
        {
            field(digest, key);
            field(digest, value);
        });
        return digest.digest();
    }

    /**
     * Whether the entry's hash is the one its fields give, after an entry
     * with a given hash.
     *
     * @param previous the hash of the entry before
     * @return true if it is
     */
    public boolean follows(byte[] previous)
    {
        return MessageDigest.isEqual(hash, hash(previous, chainId, seq, at,
                name, severity, attributes));
    }

    /**
     * The attributes as JSON, as the journal stores them.
     *
     * @return a JSON object of strings, in key order
     */
    public String attributesJson()
    {
        return JSONObjectUtils.toJSONString(attributes);
    }

    /**
     * Reads attributes stored as {@link #attributesJson()} wrote them.
     *
     * @param json the JSON object
     * @return the attributes
     * @throws IllegalArgumentException if it isn't an object of strings
     */
    public static SortedMap<String, String> attributesFromJson(String json)
    {
        Map<String, Object> parsed;
        try
        {
            parsed = JSONObjectUtils.parse(json);
        }
        catch (ParseException malformed)
        {
            throw new IllegalArgumentException("Audit attributes aren't JSON",
                    malformed);
        }
        SortedMap<String, String> attributes = new TreeMap<>();
        parsed.forEach((key, value) ->
        {
            if (!(value instanceof String text))
            {
                throw new IllegalArgumentException(
                        "Audit attributes must be strings");
            }
            attributes.put(key, text);
        });
        return attributes;
    }

    /**
     * A time in microseconds since the epoch.
     *
     * @param at the time
     * @return its microseconds
     */
    static long micros(Instant at)
    {
        return Math.addExact(Math.multiplyExact(at.getEpochSecond(),
                1_000_000L), at.getNano() / 1_000L);
    }

    /**
     * Adds a field to a hash: its length in bytes, then its UTF-8 bytes.
     *
     * @param digest the hash being computed
     * @param value  the field
     */
    private static void field(MessageDigest digest, String value)
    {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES)
                .putInt(bytes.length).array());
        digest.update(bytes);
    }
}
