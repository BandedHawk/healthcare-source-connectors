/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * The platform hardening baseline ({@code docs/security/HARDENING.md},
 * HIPAA.md §11.2 D4) says what the product's JVMs and pods are hardened
 * with, and it stays true: every kit's JVM options are in its §1, and every
 * Deployment in the kits' READMEs has §1's pod settings, so a flag dropped
 * or a Deployment added without them fails here.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class HardeningGuideTest
{

    /** The repository's root: the parent of the module's directory. */
    private static final Path ROOT = Path.of("").toAbsolutePath()
            .getParent();

    /**
     * The kits: this repository's applications, and the transform stage's
     * from its own repository's checkout, when it is beside this one.
     */
    private static final List<Path> KITS = List.of(
            ROOT.resolve("landing-connector/src/dist"),
            ROOT.resolve("file-connector/src/dist"),
            ROOT.resolve("convert-stage/src/dist"),
            ROOT.resolveSibling("healthcare-data-transformer")
                    .resolve("transform-stage/src/dist"));

    /** A README's YAML blocks, whatever their fence's label. */
    private static final Pattern YAML = Pattern.compile(
            "```yaml[^\\n]*\\n(.*?)```", Pattern.DOTALL);

    /** The pod settings §1 names. */
    private static final List<String> SETTINGS = List.of("runAsNonRoot",
            "seccompProfile", "allowPrivilegeEscalation", "capabilities",
            "readOnlyRootFilesystem", "hostPath");

    /** Creates the tests; JUnit runs each test on a new instance. */
    HardeningGuideTest()
    {
    }

    /**
     * Every option in every kit's {@code jvm.options} is named in §1, in
     * code quotes.
     *
     * @throws IOException if a file can't be read
     */
    @Test
    void namesEveryJvmFlag() throws IOException
    {
        String shipped = section();
        Set<String> flags = new TreeSet<>();
        for (Path kit : kits())
        {
            for (String line : Files.readAllLines(
                    kit.resolve("config/jvm.options")))
            {
                String flag = line.strip();
                if (!flag.isEmpty() && !flag.startsWith("#"))
                {
                    flags.add(flag);
                }
            }
        }
        assertThat(flags).as("the kits' JVM options")
                .contains("-XX:+DisableAttachMechanism");
        assertThat(flags).as("JVM options §1 leaves out")
                .allSatisfy(flag -> assertThat(shipped)
                        .contains("`" + flag + "`"));
    }

    /**
     * Every Deployment in the kits' READMEs, the stages' and the control
     * APIs', meets the restricted Pod Security Standard as §1's table has
     * it, and §1 names each setting checked.
     *
     * @throws IOException if a file can't be read
     */
    @Test
    void everyKitDeploymentHasThePodSettings() throws IOException
    {
        String shipped = section();
        assertThat(SETTINGS).as("pod settings §1 leaves out")
                .allSatisfy(setting -> assertThat(shipped)
                        .contains("`" + setting + "`"));
        List<String> problems = new ArrayList<>();
        List<String> checked = new ArrayList<>();
        for (Path kit : kits())
        {
            Matcher block = YAML.matcher(
                    Files.readString(kit.resolve("README.md")));
            while (block.find())
            {
                for (Object document : new Yaml().loadAll(block.group(1)))
                {
                    if (document instanceof Map<?, ?> resource
                            && "Deployment".equals(resource.get("kind")))
                    {
                        String name = kit.getParent().getParent()
                                .getFileName() + " "
                                + map(resource.get("metadata")).get("name");
                        checked.add(name);
                        check(name, map(map(map(resource.get("spec"))
                                .get("template")).get("spec")), problems);
                    }
                }
            }
        }
        assertThat(checked).as("the kits' Deployments").hasSizeGreaterThan(
                5);
        assertThat(problems).as("Deployments short of §1's settings")
                .isEmpty();
    }

    /**
     * Checks a pod's settings, and each of its containers'.
     *
     * @param name     the Deployment, for the problems
     * @param pod      its pod template's spec
     * @param problems where what is wrong is added
     */
    private static void check(String name, Map<String, Object> pod,
            List<String> problems)
    {
        Map<String, Object> context = map(pod.get("securityContext"));
        require(name, "runAsNonRoot", Boolean.TRUE.equals(
                context.get("runAsNonRoot")), problems);
        require(name, "seccompProfile RuntimeDefault", "RuntimeDefault"
                .equals(map(context.get("seccompProfile")).get("type")),
                problems);
        for (String host : List.of("hostNetwork", "hostPID", "hostIPC"))
        {
            require(name, "no " + host, !Boolean.TRUE.equals(pod.get(host)),
                    problems);
        }
        for (Object volume : list(pod.get("volumes")))
        {
            require(name, "no hostPath", !map(volume).containsKey(
                    "hostPath"), problems);
        }
        List<Object> containers = new ArrayList<>(list(pod.get(
                "containers")));
        containers.addAll(list(pod.get("initContainers")));
        require(name, "containers", !containers.isEmpty(), problems);
        for (Object each : containers)
        {
            Map<String, Object> container = map(each);
            String where = name + "/" + container.get("name");
            Map<String, Object> own = map(container.get("securityContext"));
            require(where, "allowPrivilegeEscalation false", Boolean.FALSE
                    .equals(own.get("allowPrivilegeEscalation")), problems);
            require(where, "capabilities drop ALL", list(map(own.get(
                    "capabilities")).get("drop")).contains("ALL"), problems);
            require(where, "readOnlyRootFilesystem", Boolean.TRUE.equals(
                    own.get("readOnlyRootFilesystem")), problems);
            require(where, "a memory limit", map(map(container.get(
                    "resources")).get("limits")).get("memory") != null,
                    problems);
        }
    }

    /**
     * Adds a problem if a setting is missing.
     *
     * @param where    the Deployment or container
     * @param setting  what is needed
     * @param present  whether it is there
     * @param problems where a problem is added
     */
    private static void require(String where, String setting,
            boolean present, List<String> problems)
    {
        if (!present)
        {
            problems.add(where + ": " + setting);
        }
    }

    /**
     * The kits there are: the transform stage's only when its repository
     * is checked out beside this one.
     *
     * @return their {@code src/dist} directories
     */
    private static List<Path> kits()
    {
        return KITS.stream().filter(Files::isDirectory).toList();
    }

    /**
     * Reads the document's §1, what the product ships.
     *
     * @return its text
     * @throws IOException if it can't be read
     */
    private static String section() throws IOException
    {
        String guide = Files.readString(
                ROOT.resolve("docs/security/HARDENING.md"));
        return guide.substring(guide.indexOf("## 1."),
                guide.indexOf("## 2."));
    }

    /**
     * A YAML mapping, as parsed; empty where there is none.
     *
     * @param value what was parsed, or null
     * @return it as a map
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value)
    {
        return value instanceof Map<?, ?> found
                ? (Map<String, Object>) found : Map.of();
    }

    /**
     * A YAML sequence, as parsed; empty where there is none.
     *
     * @param value what was parsed, or null
     * @return it as a list
     */
    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value)
    {
        return value instanceof List<?> found ? (List<Object>) found
                : List.of();
    }
}
