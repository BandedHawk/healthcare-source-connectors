/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.config;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A node's settings as support may see them (CONTROL-API-DESIGN.md §5,
 * HIPAA.md §8.1 item 11): every setting in force, each secret replaced by
 * {@value #REDACTED}. Settings hold no PHI by design; secrets are what is
 * removed.
 *
 * <p>It errs on removing. A setting is a secret if a part of its name
 * names one (a password, token, secret, credential, private key, JAAS
 * configuration and the like), or if its value holds a PEM private key.
 * In every other value, a URI's user information and any query parameter
 * whose name names a secret are removed, so an endpoint URI keeps its host
 * and options but loses its password.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class SettingsView
{

    /** What a secret's value is replaced with. */
    public static final String REDACTED = "<redacted>";

    /** Words that make a name a secret's wherever they are in it. */
    private static final List<String> SECRET_WORDS = List.of("password",
            "passwd", "passphrase", "secret", "token", "credential",
            "private", "apikey", "api-key", "api_key", "access-key",
            "accesskey", "access_key", "jaas", "connection-string",
            "connectionstring", "authorization", "client-key");

    /**
     * Short words that make a name a secret's only as a whole word,
     * between dashes, dots or underscores.
     */
    private static final Set<String> SECRET_PARTS = Set.of("pin", "sas",
            "pwd", "pass");

    /** A URI's user information: {@code scheme://user:password@}. */
    private static final Pattern USER_INFO =
            Pattern.compile("(?i)([a-z][a-z0-9+.-]*://)[^/@\\s]*@");

    /** A query or option parameter: {@code name=value}, up to a separator. */
    private static final Pattern PARAMETER =
            Pattern.compile("([?&;]|^)([A-Za-z0-9_.-]+)=([^&;]*)");

    /** A PEM private key's armour. */
    private static final Pattern PRIVATE_PEM =
            Pattern.compile("-----BEGIN [A-Z ]*PRIVATE KEY-----");

    /** Not instantiated. */
    private SettingsView()
    {
    }

    /**
     * The settings with their secrets removed.
     *
     * @param settings the settings in force, by canonical name
     * @return them, sorted by name, each secret {@value #REDACTED}
     */
    public static Map<String, String> redacted(Map<String, String> settings)
    {
        Map<String, String> view = new TreeMap<>();
        settings.forEach((name, value) -> view.put(name, redact(name,
                value)));
        return view;
    }

    /**
     * A setting's value as support may see it.
     *
     * @param name  its canonical name
     * @param value its value; null if unset
     * @return the value, or {@value #REDACTED}, or with its URIs' secrets
     *         removed
     */
    public static String redact(String name, String value)
    {
        if (value == null)
        {
            return null;
        }
        if (secret(name) || PRIVATE_PEM.matcher(value).find())
        {
            return REDACTED;
        }
        String scrubbed = USER_INFO.matcher(value).replaceAll(
                "$1" + Matcher.quoteReplacement(REDACTED) + "@");
        Matcher parameters = PARAMETER.matcher(scrubbed);
        StringBuilder out = new StringBuilder();
        while (parameters.find())
        {
            String replacement = secret(parameters.group(2))
                    ? parameters.group(1) + parameters.group(2) + "="
                            + REDACTED
                    : parameters.group();
            parameters.appendReplacement(out,
                    Matcher.quoteReplacement(replacement));
        }
        parameters.appendTail(out);
        return out.toString();
    }

    /**
     * Whether a name is a secret's: it holds a word that names one, or a
     * part of it, between dashes, dots or underscores, is a short one
     * such as {@code pin}.
     *
     * @param name the name
     * @return true if it is
     */
    public static boolean secret(String name)
    {
        String lower = name.toLowerCase(Locale.ROOT);
        for (String word : SECRET_WORDS)
        {
            if (lower.contains(word))
            {
                return true;
            }
        }
        for (String part : lower.split("[-._\\[\\]]"))
        {
            if (SECRET_PARTS.contains(part))
            {
                return true;
            }
        }
        return false;
    }
}
