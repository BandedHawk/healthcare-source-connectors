/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.runtime.config.CommonSettings;
import com.sdh.healthcare.connector.stage.StageProperties;
import java.io.IOException;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Retention and destruction ({@code docs/security/RETENTION.md}, HIPAA.md
 * §11.2 D9) says what each removal deletes, and it stays true of the code:
 * every retention setting is a row with its default, every command a data
 * or audit admin runs is named, and every claim of the erasure certificate
 * is in its table, so a removal added, or a claim, can't be left out of
 * what the customer's disposal procedures rely on.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RetentionGuideTest
{

    /** The repository's root: the parent of the module's directory. */
    private static final Path ROOT = Path.of("").toAbsolutePath()
            .getParent();

    /**
     * Where a stage names the setting a table's retention comes from:
     * {@code MaintenanceScheduler.retention("<setting>", ...)}.
     */
    private static final Pattern TABLE_RETENTION = Pattern.compile(
            "MaintenanceScheduler\\.retention\\(\\s*\"([^\"]+)\"");

    /** A claim the erasure certificate is given. */
    private static final Pattern CLAIM = Pattern.compile(
            "claims\\.put\\(\"([A-Za-z]+)\"");

    /** The settings of maintenance's own removals, by their defaults. */
    private static final Map<String, String> MAINTENANCE = Map.of(
            "sdh.maintenance.retention.given-up", "7d",
            "sdh.maintenance.snapshot-retention", "24h",
            "sdh.maintenance.min-snapshots", "2000",
            "sdh.maintenance.orphans.min-age", "3d");

    /** Creates the tests; JUnit runs each test on a new instance. */
    RetentionGuideTest()
    {
    }

    /**
     * Every setting a table's retention is read from, in this repository's
     * stages, the retention bounds, and the registry's retention settings
     * are rows of the document's tables, with their defaults, as
     * {@code | `<name>` | <default> |}; {@code unset} where there is none.
     *
     * @throws IOException if a file can't be read
     */
    @Test
    void namesEveryRetentionSetting() throws IOException
    {
        Map<String, String> settings = new TreeMap<>(MAINTENANCE);
        for (Path source : sources())
        {
            Matcher named = TABLE_RETENTION.matcher(Files.readString(source));
            while (named.find())
            {
                settings.put(named.group(1), "unset");
            }
        }
        assertThat(settings).as("the tables' retention settings found")
                .containsKeys("sdh.maintenance.retention",
                        "sdh.maintenance.retention.landed-items",
                        "sdh.maintenance.retention.records");
        defaults(CommonSettings.RetentionBounds.class,
                "sdh.maintenance.retention-bounds", settings);
        defaults(StageProperties.Retention.class,
                StageProperties.PREFIX + ".retention", settings);
        String guide = guide();
        Set<String> missing = new TreeSet<>();
        settings.forEach((setting, value) ->
        {
            if (!guide.contains("\n| `" + setting + "` | " + value + " |"))
            {
                missing.add(setting + " (" + value + ")");
            }
        });
        assertThat(missing).as("retention settings RETENTION.md leaves out")
                .isEmpty();
    }

    /**
     * Every command that needs {@code data.admin} or {@code audit.admin},
     * the permissions that set retention, purge, erase, restore and prune,
     * is named in the document.
     *
     * @throws IOException if the document can't be read
     */
    @Test
    void namesEveryDataAndAuditAdminCommand() throws IOException
    {
        List<AdminCommand<?>> commands = new ArrayList<>();
        commands.addAll(DataCommands.<DataAdmin>all());
        commands.addAll(RestoreCommands.<DataAdmin>all());
        commands.addAll(AuditCommands.all());
        Set<String> admin = new TreeSet<>();
        for (AdminCommand<?> command : commands)
        {
            if (command instanceof Operation<?, ?, ?> operation
                    && Set.of(Permission.DATA_ADMIN, Permission.AUDIT_ADMIN)
                            .contains(operation.permission()))
            {
                admin.add(command.name());
            }
        }
        assertThat(admin).as("the data and audit admins' commands")
                .contains("purge", "erase", "audit-prune");
        String guide = guide();
        assertThat(admin).as("commands RETENTION.md leaves out")
                .allSatisfy(name -> assertThat(guide)
                        .contains("`" + name + "`"));
    }

    /**
     * Every claim the erasure certificate is given is in §2.5's table.
     *
     * @throws IOException if a file can't be read
     */
    @Test
    void namesEveryCertificateClaim() throws IOException
    {
        Matcher put = CLAIM.matcher(Files.readString(ROOT.resolve(
                "connector-stage/src/main/java/com/sdh/healthcare/connector"
                        + "/stage/admin/DataCommands.java")));
        Set<String> claims = new TreeSet<>();
        while (put.find())
        {
            claims.add(put.group(1));
        }
        assertThat(claims).as("the certificate's claims")
                .contains("keks", "signingKid", "journalHeads");
        String guide = guide();
        String erasure = guide.substring(guide.indexOf("### 2.5"),
                guide.indexOf("### 2.6"));
        assertThat(claims).as("claims §2.5 leaves out")
                .allSatisfy(claim -> assertThat(erasure)
                        .contains("`" + claim + "`"));
    }

    /**
     * Reads the document.
     *
     * @return its text
     * @throws IOException if it can't be read
     */
    private static String guide() throws IOException
    {
        return Files.readString(ROOT.resolve("docs/security/RETENTION.md"));
    }

    /**
     * This repository's main Java sources.
     *
     * @return their paths
     * @throws IOException if the tree can't be walked
     */
    private static List<Path> sources() throws IOException
    {
        try (Stream<Path> walk = Files.walk(ROOT))
        {
            return walk.filter(path -> path.toString().endsWith(".java")
                    && path.toString().contains("/src/main/java/"))
                    .toList();
        }
    }

    /**
     * Adds a record's settings, with their defaults: {@code unset} where it
     * has none.
     *
     * @param type     the record
     * @param prefix   its settings' prefix
     * @param settings where they are added, by setting
     */
    private static void defaults(Class<?> type, String prefix,
            Map<String, String> settings)
    {
        RecordComponent[] components = type.getRecordComponents();
        Class<?>[] types = new Class<?>[components.length];
        for (int i = 0; i < components.length; i++)
        {
            types[i] = components[i].getType();
        }
        Parameter[] parameters;
        try
        {
            parameters = type.getDeclaredConstructor(types).getParameters();
        }
        catch (NoSuchMethodException e)
        {
            throw new IllegalStateException(type + " has no canonical"
                    + " constructor", e);
        }
        for (int i = 0; i < components.length; i++)
        {
            DefaultValue value = parameters[i]
                    .getAnnotation(DefaultValue.class);
            settings.put(prefix + "." + kebab(components[i].getName()),
                    value == null ? "unset" : value.value()[0]);
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
