/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.stage.control.ControlRoute;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * The audit event catalogue ({@code docs/security/AUDIT-EVENTS.md},
 * HIPAA.md §11.2 D7) lists every audit event the product emits, so an event
 * added to the code can't be left out of what the security analyst is told.
 * The events are found in the main sources of every module, and of the
 * transformer's checkout when it is beside this one.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class AuditEventsCatalogueTest
{

    /** The repository's root: the parent of the module's directory. */
    private static final Path ROOT = Path.of("").toAbsolutePath()
            .getParent();

    /** The transformer's checkout, beside this one. */
    private static final Path TRANSFORMER = ROOT.resolveSibling(
            "healthcare-data-transformer");

    /** The catalogue. */
    private static final Path CATALOGUE = ROOT.resolve(
            "docs/security/AUDIT-EVENTS.md");

    /** The start of an audit call on {@code Telemetry}. */
    private static final String AUDIT =
            "(?:Telemetry\\.get\\(\\)|telemetry)\\s*\\.\\s*"
            + "audit(?:Warn|Error)?\\(\\s*";

    /**
     * An audit call given its event's name, whole (followed by a comma) or
     * as the start of one that is built (followed by {@code +}).
     */
    private static final Pattern NAMED = Pattern.compile(
            AUDIT + "\"([a-z0-9_.]+)\"\\s*([,+])");

    /** An audit call given its event's name in a variable. */
    private static final Pattern VARIABLE = Pattern.compile(
            AUDIT + "([a-z][A-Za-z0-9_]*)\\s*,");

    /**
     * A name given to a helper that audits it for its caller: {@code
     * RecordReconciler}'s recoveries, {@code StageRegistry}'s numbering and
     * {@code IcebergTableSource}'s reports.
     */
    private static final Pattern HELPER = Pattern.compile(
            "(?:new Recovery\\(|number\\(connection,|report\\()\\s*"
            + "\"([a-z0-9_.]+)\"");

    /**
     * The audit calls that take their event's name in a variable, as
     * {@code File.java:variable}: each family is in the catalogue (§6, and
     * the helpers' names above).
     */
    private static final Set<String> VARIABLE_SITES = Set.of(
            "AdminCli.java:event", "ControlService.java:event",
            "StageRegistry.java:event", "RecordReconciler.java:event",
            "IcebergTableSource.java:event");

    /**
     * The starts of the names that are built, each of whose names the
     * catalogue lists (§4.5).
     */
    private static final Set<String> BUILT = Set.of("failure.");

    /** Creates the tests; JUnit runs each test on a new instance. */
    AuditEventsCatalogueTest()
    {
    }

    /**
     * Every event named in the code is a row of the catalogue's own.
     *
     * @throws IOException if a source or the catalogue can't be read
     */
    @Test
    void listsEveryNamedEvent() throws IOException
    {
        String catalogue = Files.readString(CATALOGUE);
        Set<String> missing = new TreeSet<>();
        for (Path source : sources())
        {
            String text = Files.readString(source);
            Matcher named = NAMED.matcher(text);
            while (named.find())
            {
                if (named.group(2).equals(","))
                {
                    missing.add(named.group(1));
                }
            }
            Matcher helped = HELPER.matcher(text);
            while (helped.find())
            {
                missing.add(helped.group(1));
            }
        }
        assertThat(missing).as("events found").isNotEmpty();
        missing.removeIf(event -> catalogue.contains("\n| `" + event
                + "` |"));
        assertThat(missing).as("events the catalogue leaves out").isEmpty();
    }

    /**
     * Every event whose name is built, or given in a variable, is of a
     * family the catalogue lists.
     *
     * @throws IOException if a source or the catalogue can't be read
     */
    @Test
    void knowsEveryBuiltName() throws IOException
    {
        Set<String> unknown = new TreeSet<>();
        for (Path source : sources())
        {
            String text = Files.readString(source);
            Matcher named = NAMED.matcher(text);
            while (named.find())
            {
                if (named.group(2).equals("+")
                        && !BUILT.contains(named.group(1)))
                {
                    unknown.add(named.group(1) + " + …");
                }
            }
            Matcher variable = VARIABLE.matcher(text);
            while (variable.find())
            {
                String site = source.getFileName() + ":"
                        + variable.group(1);
                if (!VARIABLE_SITES.contains(site))
                {
                    unknown.add(site);
                }
            }
        }
        assertThat(unknown).as("names built where the catalogue doesn't"
                + " expect them: list their events, then add them here")
                .isEmpty();
        assertThat(Files.readString(CATALOGUE))
                .contains("\n| `admin.<command>` |")
                .contains("\n| `control.<operation>` |")
                .contains("\n| `failure.declined` |");
    }

    /**
     * Every control API operation, and so every {@code control.} and
     * {@code admin.} event's command, is in the catalogue's table of
     * commands (§6.1).
     *
     * @throws IOException if the catalogue can't be read
     */
    @Test
    void listsEveryCommand() throws IOException
    {
        String catalogue = Files.readString(CATALOGUE);
        int start = catalogue.indexOf("### 6.1");
        int end = catalogue.indexOf("### 6.2");
        assertThat(start).as("§6.1").isPositive();
        assertThat(end).as("§6.2").isGreaterThan(start);
        String commands = catalogue.substring(start, end);
        Set<String> missing = new TreeSet<>();
        for (ControlRoute route : ControlRoute.all())
        {
            if (!commands.contains("`" + route.operation() + "`"))
            {
                missing.add(route.operation());
            }
        }
        assertThat(missing).as("commands §6.1 leaves out").isEmpty();
    }

    /**
     * The main Java sources of every module here, and of the transformer's
     * when it is checked out beside this repository.
     *
     * @return the files
     * @throws IOException if a directory can't be listed
     */
    private static List<Path> sources() throws IOException
    {
        List<Path> roots = Files.isDirectory(TRANSFORMER)
                ? List.of(ROOT, TRANSFORMER) : List.of(ROOT);
        List<Path> found = new ArrayList<>();
        for (Path root : roots)
        {
            try (Stream<Path> modules = Files.list(root))
            {
                modules.map(module -> module.resolve("src/main/java"))
                        .filter(Files::isDirectory)
                        .forEach(main -> found.addAll(javaFiles(main)));
            }
        }
        return found;
    }

    /**
     * The Java files under a directory.
     *
     * @param directory the directory
     * @return its {@code .java} files, at any depth
     * @throws UncheckedIOException if it can't be walked
     */
    private static List<Path> javaFiles(Path directory)
    {
        try (Stream<Path> files = Files.walk(directory))
        {
            return files.filter(file -> file.toString().endsWith(".java"))
                    .toList();
        }
        catch (IOException failure)
        {
            throw new UncheckedIOException(failure);
        }
    }
}
