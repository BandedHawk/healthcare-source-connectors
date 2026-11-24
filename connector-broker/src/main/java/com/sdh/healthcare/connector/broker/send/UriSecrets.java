/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.broker.send;

import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.camel.util.URISupport;

/**
 * The refusal of a secret written into a broker URI (design §5.3.16),
 * for the URIs a node receives from and the ones it sends to alike: a
 * password, key, secret, token or connection string must be a property
 * placeholder ({@code {{...}}}), resolved from the environment when the
 * route starts, or a bean reference ({@code #name}), so that it is never
 * in the settings themselves. A refusal names the option, never its value.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class UriSecrets
{

    /** Option names that hold secrets, in lower case. */
    private static final Set<String> SECRET_PARTS = Set.of("password",
            "secret", "jaas", "accesskey", "sessiontoken", "token",
            "connectionstring");

    /**
     * A property placeholder, resolved when the route starts, or a bean
     * reference.
     */
    private static final Pattern PLACEHOLDER = Pattern.compile(
            "(RAW\\()?\\{\\{[^{}]+\\}\\}\\)?|#[A-Za-z0-9_.-]+");

    /** Not instantiated. */
    private UriSecrets()
    {
    }

    /**
     * Refuses a URI with a secret written into its query.
     *
     * @param uri     the Camel endpoint URI
     * @param setting the setting it comes from, for the message
     * @throws IllegalArgumentException if its query is malformed, or an
     *                                  option holding a secret is a value
     */
    public static void refuseWritten(String uri, String setting)
    {
        int question = uri.indexOf('?');
        if (question < 0)
        {
            return;
        }
        Map<String, Object> options;
        try
        {
            options = URISupport.parseQuery(uri.substring(question + 1),
                    true);
        }
        catch (URISyntaxException bad)
        {
            throw new IllegalArgumentException(
                    setting + " has a malformed query");
        }
        refuseWritten(options, setting);
    }

    /**
     * Refuses a URI's options if one holding a secret is a value.
     *
     * @param options the URI's options, as Camel parses them
     * @param setting the setting they come from, for the message
     * @throws IllegalArgumentException if an option holding a secret is a
     *                                  value
     */
    public static void refuseWritten(Map<String, Object> options,
            String setting)
    {
        for (Map.Entry<String, Object> option : options.entrySet())
        {
            String name = option.getKey().toLowerCase(Locale.ROOT);
            if (SECRET_PARTS.stream().anyMatch(name::contains)
                    && !PLACEHOLDER.matcher(
                            String.valueOf(option.getValue())).matches())
            {
                throw new IllegalArgumentException(setting + " option "
                        + option.getKey() + " must be a property"
                        + " placeholder ({{...}}), not a value");
            }
        }
    }
}
