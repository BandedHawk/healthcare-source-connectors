/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import com.nimbusds.jose.util.JSONObjectUtils;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Finds the patients a FHIR resource names: the {@code identifier}s of
 * every {@code Patient} in it, whether it is a Patient, a Bundle's entry or
 * a contained resource, at any depth. For the patient lookup (HIPAA.md
 * §4.2, §5): what an operator matches against the covered entity's own
 * patient index, nothing more.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class PatientIdentifiers
{

    /** Not instantiated. */
    private PatientIdentifiers()
    {
    }

    /**
     * A patient identifier.
     *
     * @param system its namespace URI; empty if the identifier has none, or
     *               for a value to match under any system
     * @param value  its value
     */
    record Identifier(String system, String value)
    {

        /**
         * Reads an identifier as an operator writes one: {@code
         * system|value}, or {@code value} alone for any system.
         *
         * @param line the text
         * @return the identifier
         */
        static Identifier parse(String line)
        {
            int bar = line.indexOf('|');
            return bar < 0 ? new Identifier("", line)
                    : new Identifier(line.substring(0, bar),
                            line.substring(bar + 1));
        }

        /**
         * Whether this identifier, as one to look for, matches one found:
         * the same value, and the same system unless this names none.
         *
         * @param found an identifier found in a record
         * @return true if it matches
         */
        boolean matches(Identifier found)
        {
            return value.equals(found.value())
                    && (system.isEmpty() || system.equals(found.system()));
        }
    }

    /**
     * Finds the patient identifiers in a FHIR JSON resource.
     *
     * @param json the resource, UTF-8
     * @return its patients' identifiers, in the order found
     * @throws IllegalArgumentException if it isn't a JSON object
     */
    static Set<Identifier> in(byte[] json)
    {
        Map<String, Object> root;
        try
        {
            root = JSONObjectUtils.parse(
                    new String(json, StandardCharsets.UTF_8), -1);
        }
        catch (ParseException malformed)
        {
            throw new IllegalArgumentException("Not a JSON object");
        }
        Set<Identifier> found = new LinkedHashSet<>();
        Deque<Object> pending = new ArrayDeque<>();
        pending.push(root);
        while (!pending.isEmpty())
        {
            Object next = pending.pop();
            if (next instanceof Map<?, ?> object)
            {
                if ("Patient".equals(object.get("resourceType")))
                {
                    identifiers(object.get("identifier"), found);
                }
                object.values().forEach(pending::push);
            }
            else if (next instanceof List<?> list)
            {
                list.forEach(pending::push);
            }
        }
        return found;
    }

    /**
     * Adds a Patient's identifiers.
     *
     * @param identifiers its {@code identifier} element
     * @param found       where they are added
     */
    private static void identifiers(Object identifiers, Set<Identifier> found)
    {
        if (!(identifiers instanceof List<?> list))
        {
            return;
        }
        for (Object each : list)
        {
            if (each instanceof Map<?, ?> identifier
                    && identifier.get("value") instanceof String value)
            {
                found.add(new Identifier(identifier.get("system")
                        instanceof String system ? system : "", value));
            }
        }
    }
}
