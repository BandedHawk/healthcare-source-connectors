/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.crypto;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Writes and reads a value as a JSON object that carries its own MAC, for
 * state kept in table properties, such as a checkpoint. The object's
 * {@value #MAC_KEY} field holds
 * {@code HMAC(idKey, label ‖ 0 ‖ the canonical JSON without the MAC)}, so
 * anyone without the ID key who changes the value is found out.
 * <p>
 * The MAC is checked against a canonical form rebuilt from the parsed value,
 * so it doesn't depend on how the stored JSON is formatted.
 *
 * @param <T> the type of the value
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class MacedJson<T>
{

    /** The JSON field that holds the MAC. */
    private static final String MAC_KEY = "mac";

    /** Makes the MACs, with the ID key. */
    private final RecordIds ids;

    /**
     * What kind of value this is, fed to the MAC first, so a MAC made for
     * one kind of value is never valid for another.
     */
    private final String label;

    /** Turns a value into its fields, in a fixed order. */
    private final Function<T, Map<String, Object>> toCanonical;

    /** Rebuilds a value from its parsed fields. */
    private final Function<Map<String, Object>, T> fromJson;

    /**
     * Creates a reader and writer for one kind of value.
     *
     * @param ids         makes the MACs, with the ID key
     * @param label       what kind of value this is, such as
     *                    {@code checkpoint}
     * @param toCanonical turns a value into its fields, in a fixed order;
     *                    the values must be strings, numbers, booleans or
     *                    null
     * @param fromJson    rebuilds a value from its parsed fields; may throw
     *                    if a field is missing or malformed
     */
    public MacedJson(RecordIds ids, String label,
            Function<T, Map<String, Object>> toCanonical,
            Function<Map<String, Object>, T> fromJson)
    {
        this.ids = ids;
        this.label = label;
        this.toCanonical = toCanonical;
        this.fromJson = fromJson;
    }

    /**
     * Writes a value as JSON with its MAC.
     *
     * @param value the value
     * @return the JSON object, as text
     */
    public String encode(T value)
    {
        Map<String, Object> fields =
                new LinkedHashMap<>(toCanonical.apply(value));
        fields.put(MAC_KEY, mac(fields));
        return JSONObjectUtils.toJSONString(fields);
    }

    /**
     * Reads a value written by {@link #encode}, and checks its MAC.
     *
     * @param json the JSON object, as text
     * @return the value
     * @throws Tampered if the MAC doesn't verify or the content can't be
     *                  parsed
     */
    public T decode(String json)
    {
        T value;
        String storedMac;
        try
        {
            Map<String, Object> fields = JSONObjectUtils.parse(json);
            value = fromJson.apply(fields);
            storedMac = JSONObjectUtils.getString(fields, MAC_KEY);
        }
        catch (ParseException | RuntimeException unreadable)
        {
            throw new Tampered();
        }
        String expectedMac = mac(toCanonical.apply(value));
        // A constant-time comparison, so timing reveals nothing of the MAC.
        if (storedMac == null || !MessageDigest.isEqual(
                storedMac.getBytes(StandardCharsets.US_ASCII),
                expectedMac.getBytes(StandardCharsets.US_ASCII)))
        {
            throw new Tampered();
        }
        return value;
    }

    /**
     * Makes the MAC of a value's fields.
     *
     * @param canonical the fields, in a fixed order; a MAC field among them
     *                  is left out
     * @return the MAC, in unpadded base64url
     */
    private String mac(Map<String, Object> canonical)
    {
        Map<String, Object> fields = new LinkedHashMap<>(canonical);
        fields.remove(MAC_KEY);
        byte[] json = JSONObjectUtils.toJSONString(fields)
                .getBytes(StandardCharsets.UTF_8);
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(ids.newLabelledMac(label).doFinal(json));
    }

    /**
     * The stored value has been changed, or can't be read. Thrown by
     * {@link #decode}; callers turn it into their own error. It says
     * nothing about the content.
     */
    public static final class Tampered extends RuntimeException
    {

        /** Creates the exception, with a fixed message. */
        Tampered()
        {
            super("MAC verification failed");
        }
    }
}
