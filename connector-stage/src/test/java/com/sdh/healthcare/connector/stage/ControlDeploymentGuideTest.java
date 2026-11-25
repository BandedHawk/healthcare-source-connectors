/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.stage.control.ControlSettings;
import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * The control API deployment document
 * ({@code docs/security/CONTROL-API-DEPLOYMENT.md}, HIPAA.md §11.2 D13)
 * describes every control API setting, so a setting added to
 * {@link ControlSettings} can't be left out of what the platform
 * administrator is told to set.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ControlDeploymentGuideTest
{

    /** The repository's root: the parent of the module's directory. */
    private static final Path ROOT = Path.of("").toAbsolutePath()
            .getParent();

    /** Creates the tests; JUnit runs each test on a new instance. */
    ControlDeploymentGuideTest()
    {
    }

    /**
     * Every setting, nested ones by their full name, is a row of §4's
     * table, as {@code | `sdh.control.<name>` |}.
     *
     * @throws IOException if the document can't be read
     */
    @Test
    void describesEverySetting() throws IOException
    {
        String guide = Files.readString(ROOT.resolve(
                "docs/security/CONTROL-API-DEPLOYMENT.md"));
        Set<String> settings = new TreeSet<>();
        settings(ControlSettings.class, "sdh.control", settings);
        assertThat(settings).as("the settings found").contains(
                "sdh.control.auth.jwt.issuer-uri",
                "sdh.control.approval.break-glass-ttl");
        Set<String> missing = new TreeSet<>();
        for (String setting : settings)
        {
            if (!guide.contains("\n| `" + setting + "` |"))
            {
                missing.add(setting);
            }
        }
        assertThat(missing).as("settings CONTROL-API-DEPLOYMENT.md §4"
                + " leaves out").isEmpty();
    }

    /**
     * Adds a record's settings: each component by its name, and a nested
     * record's components under it.
     *
     * @param type     the record
     * @param prefix   its settings' prefix
     * @param settings where they are added
     */
    private static void settings(Class<?> type, String prefix,
            Set<String> settings)
    {
        for (RecordComponent component : type.getRecordComponents())
        {
            String name = prefix + "." + kebab(component.getName());
            if (component.getType().isRecord())
            {
                settings(component.getType(), name, settings);
            }
            else
            {
                settings.add(name);
            }
        }
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
