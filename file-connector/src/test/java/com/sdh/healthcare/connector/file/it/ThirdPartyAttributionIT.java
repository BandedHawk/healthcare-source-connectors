/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The third-party attribution of each application's packaged jar, checked
 * against what the jar bundles (design §14.2), without Docker: the image
 * tests check the same files reach {@code /app/legal/} in the images. For
 * every bundled component:
 * <ul>
 *   <li>it has an entry in {@code THIRD-PARTY.txt};</li>
 *   <li>every license, notice or copyright file it ships, wherever it is
 *       in its jar, is in that entry by its path: such files carry the
 *       copyright lines MIT, BSD and ISC require, and the licenses of code
 *       the component bundles;</li>
 *   <li>a choice of licenses has an election that isn't a GNU license, or
 *       an {@code All apply} line where each covers part of it;</li>
 *   <li>every license text referenced is there, and isn't the placeholder
 *       a build without network access writes.</li>
 * </ul>
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ThirdPartyAttributionIT
{

    /**
     * What counts as a license, notice or copyright file: the same rule as
     * the attribution step in the parent POM ({@code legalFile} and
     * {@code sourceFile}).
     */
    static final Pattern LEGAL_FILE = Pattern.compile(
            "(?i)(.*/)?(LICEN[CS]E|NOTICE|COPYING|COPYRIGHT)([._-][^/]*)?");

    /** Source and class files whose names only look like license files. */
    static final Pattern SOURCE_FILE =
            Pattern.compile("(?i).*\\.(class|java|kt|groovy|scala)");

    /** The component's primary NOTICE, given in its NOTICE section. */
    static final Pattern PRIMARY_NOTICE =
            Pattern.compile("(?i)(META-INF/)?NOTICE(\\.TXT|\\.MD)?");

    /** Where the attribution sits in an application's jar. */
    static final String META_INF = "META-INF/";

    /** What the attribution step writes for a text it couldn't get. */
    static final List<String> PLACEHOLDERS = List.of(
            "text unavailable during build",
            "License text not provided by the dependency POM",
            "PLACEHOLDER");

    /** Creates the test; JUnit runs each test on a new instance. */
    ThirdPartyAttributionIT()
    {
    }

    /**
     * Every component the jar bundles is attributed in full.
     *
     * @param module the application's module
     * @throws Exception if the jar can't be read
     */
    @ParameterizedTest
    @ValueSource(strings = {"file-connector", "landing-connector"})
    void everyBundledComponentIsAttributedInFull(String module)
            throws Exception
    {
        try (ZipFile jar = new ZipFile(packagedJar(module).toFile()))
        {
            String thirdParty = read(jar, META_INF + "THIRD-PARTY.txt");
            Map<String, String> entries = entries(thirdParty);
            List<String> missing = new ArrayList<>();
            Enumeration<? extends ZipEntry> all = jar.entries();
            int components = 0;
            while (all.hasMoreElements())
            {
                ZipEntry nested = all.nextElement();
                String name = nested.getName();
                if (!name.startsWith("BOOT-INF/lib/")
                        || !name.endsWith(".jar"))
                {
                    continue;
                }
                String file = name.substring("BOOT-INF/lib/".length());
                String entry = entryFor(entries, file);
                if (entry == null)
                {
                    // Only our own modules may be left out.
                    assertThat(file).as("attributed in THIRD-PARTY.txt")
                            .matches("connector-[a-z-]+-[0-9].*\\.jar");
                    continue;
                }
                components++;
                for (String legal : legalFiles(jar, nested))
                {
                    if (!entry.contains("--- " + legal
                            + " (as shipped by the component) ---"))
                    {
                        missing.add(file + "!/" + legal);
                    }
                }
            }
            assertThat(components).isGreaterThan(300);
            assertThat(missing).as("license files missing from "
                    + module + "'s THIRD-PARTY.txt").isEmpty();

            for (String entry : entries.values())
            {
                Matcher licenses = Pattern
                        .compile("(?m)^Licenses: (.*)$").matcher(entry);
                assertThat(licenses.find()).as(entry).isTrue();
                if (licenses.group(1).contains(","))
                {
                    assertThat(entry).as("a choice decided")
                            .containsAnyOf("\nElected license: ",
                                    "\nAll apply: ");
                    Matcher elected = Pattern
                            .compile("(?m)^Elected license: (.*)$")
                            .matcher(entry);
                    if (elected.find())
                    {
                        assertThat(elected.group(1)).as(entry)
                                .doesNotContain("NONE")
                                .doesNotContain("GPL");
                    }
                }
            }

            Matcher references = Pattern.compile(
                    "META-INF/licenses/([^\\s,]+\\.txt)").matcher(thirdParty);
            List<String> texts = new ArrayList<>();
            while (references.find())
            {
                if (!texts.contains(references.group(1)))
                {
                    texts.add(references.group(1));
                }
            }
            assertThat(texts).contains("apache-2.0.txt", "mit.txt");
            for (String text : texts)
            {
                String content = read(jar, META_INF + "licenses/" + text);
                assertThat(content).as(text).hasSizeGreaterThan(200);
                for (String placeholder : PLACEHOLDERS)
                {
                    assertThat(content).as(text)
                            .doesNotContain(placeholder);
                }
            }
            for (String placeholder : PLACEHOLDERS)
            {
                assertThat(thirdParty).doesNotContain(placeholder);
            }
        }
    }

    /**
     * The landing node's jar holds JSch, whose bundled jBCrypt is under
     * ISC: all of JSch's licenses apply, and jBCrypt's copyright is given.
     *
     * @throws Exception if the jar can't be read
     */
    @ParameterizedTest
    @ValueSource(strings = {"landing-connector"})
    void bundledCodeKeepsItsOwnCopyright(String module) throws Exception
    {
        try (ZipFile jar = new ZipFile(packagedJar(module).toFile()))
        {
            String jsch = entryFor(entries(read(jar,
                    META_INF + "THIRD-PARTY.txt")), "jsch-");
            assertThat(jsch).contains("\nAll apply: ")
                    .doesNotContain("Elected license")
                    .contains("Damien Miller")
                    .contains("--- META-INF/LICENSE.JZlib.txt");
        }
    }

    /**
     * An application's packaged jar. A build of file-connector alone (as
     * {@code -Pdist -pl file-connector}) hasn't packaged the landing node,
     * so its check is skipped then, rather than failed.
     *
     * @param module the application's module
     * @return the jar
     * @throws Exception if its target directory can't be listed
     */
    static Path packagedJar(String module) throws Exception
    {
        try
        {
            return ContainerImageItBase.packagedJar(module);
        }
        catch (IllegalStateException notPackaged)
        {
            assumeTrue(false, module + " isn't packaged in this build");
            throw notPackaged;
        }
    }

    /**
     * Each component's entry in {@code THIRD-PARTY.txt}, by its
     * coordinates.
     *
     * @param thirdParty the file
     * @return the entries, by {@code group:artifact:version}
     */
    static Map<String, String> entries(String thirdParty)
    {
        Map<String, String> entries = new LinkedHashMap<>();
        Matcher coordinates = Pattern.compile("(?m)^Coordinates: (\\S+)$")
                .matcher(thirdParty);
        List<Integer> starts = new ArrayList<>();
        List<String> names = new ArrayList<>();
        while (coordinates.find())
        {
            starts.add(coordinates.start());
            names.add(coordinates.group(1));
        }
        for (int index = 0; index < names.size(); index++)
        {
            int end = index + 1 < names.size()
                    ? starts.get(index + 1) : thirdParty.length();
            entries.put(names.get(index),
                    thirdParty.substring(starts.get(index), end));
        }
        return entries;
    }

    /**
     * The entry for a bundled jar, by its file name: the artifact ID and
     * version, then the classifier if any.
     *
     * @param entries the entries, by coordinates
     * @param file    the jar's file name, or the start of one
     * @return the entry; null if there is none
     */
    static String entryFor(Map<String, String> entries, String file)
    {
        String best = null;
        int bestLength = 0;
        for (Map.Entry<String, String> entry : entries.entrySet())
        {
            String[] gav = entry.getKey().split(":");
            String stem = gav[1] + "-" + gav[2];
            boolean matches = file.endsWith("-")
                    ? stem.startsWith(file)
                    : file.equals(stem + ".jar")
                            || file.startsWith(stem + "-");
            if (matches && stem.length() > bestLength)
            {
                best = entry.getValue();
                bestLength = stem.length();
            }
        }
        return best;
    }

    /**
     * The license, notice and copyright files in a bundled jar, except its
     * primary NOTICE, which has a section of its own.
     *
     * @param jar    the application's jar
     * @param nested the bundled jar's entry
     * @return their paths in the bundled jar
     * @throws IOException if it can't be read
     */
    static List<String> legalFiles(ZipFile jar, ZipEntry nested)
            throws IOException
    {
        List<String> names = new ArrayList<>();
        try (ZipInputStream in = new ZipInputStream(
                new ByteArrayInputStream(
                        jar.getInputStream(nested).readAllBytes())))
        {
            ZipEntry entry;
            while ((entry = in.getNextEntry()) != null)
            {
                String name = entry.getName();
                if (!entry.isDirectory()
                        && LEGAL_FILE.matcher(name).matches()
                        && !SOURCE_FILE.matcher(name).matches())
                {
                    names.add(name);
                }
            }
        }
        // The step gives the shortest primary NOTICE its own section.
        names.stream().filter(name -> PRIMARY_NOTICE.matcher(name)
                .matches()).min((a, b) -> a.length() - b.length())
                .ifPresent(names::remove);
        return names;
    }

    /**
     * A text file in the application's jar.
     *
     * @param jar  the jar
     * @param name the file's path in it
     * @return its content
     * @throws IOException if it is missing or can't be read
     */
    static String read(ZipFile jar, String name) throws IOException
    {
        ZipEntry entry = jar.getEntry(name);
        if (entry == null)
        {
            throw new IOException(name + " is not in " + jar.getName());
        }
        return new String(jar.getInputStream(entry).readAllBytes(), UTF_8);
    }
}
