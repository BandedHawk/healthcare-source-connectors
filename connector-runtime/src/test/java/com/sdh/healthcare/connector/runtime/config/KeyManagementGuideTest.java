/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * The key management document ({@code docs/security/KEYS.md}, HIPAA.md
 * §11.2 D3) names every key setting, so a key added to
 * {@link CommonSettings.Keys} can't be left out of what the key custodians
 * are told.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class KeyManagementGuideTest
{

    /** The repository's root: the parent of the module's directory. */
    private static final Path ROOT = Path.of("").toAbsolutePath()
            .getParent();

    /** The settings, besides each key, the document must name. */
    private static final Set<String> OTHERS = Set.of("sdh.crypto.pod-keys",
            "sdh.crypto.jose.agreement-messages", "sdh.results.recipient",
            "sdh.stage.sink.kek");

    /** Creates the tests; JUnit runs each test on a new instance. */
    KeyManagementGuideTest()
    {
    }

    /**
     * Every key setting is named in the document, as
     * {@code `sdh.crypto.keys.<key>}.
     *
     * @throws IOException if the document can't be read
     */
    @Test
    void namesEveryKey() throws IOException
    {
        String guide = Files.readString(ROOT.resolve(
                "docs/security/KEYS.md"));
        Set<String> missing = new TreeSet<>();
        for (RecordComponent key
                : CommonSettings.Keys.class.getRecordComponents())
        {
            String setting = "sdh.crypto.keys." + kebab(key.getName());
            if (!guide.contains("`" + setting))
            {
                missing.add(setting);
            }
        }
        for (String setting : OTHERS)
        {
            if (!guide.contains("`" + setting))
            {
                missing.add(setting);
            }
        }
        assertThat(missing).as("key settings KEYS.md leaves out").isEmpty();
    }

    /**
     * A record component's name as Spring's canonical setting name has it.
     *
     * @param name the name, in camel case
     * @return it in kebab case
     */
    private static String kebab(String name)
    {
        return name.replaceAll("([a-z0-9])([A-Z])", "$1-$2").toLowerCase();
    }
}
