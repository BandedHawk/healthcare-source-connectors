/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.landing.config.LandingProperties;
import com.sdh.healthcare.connector.stage.StageProperties;
import com.sdh.healthcare.connector.stage.control.ControlSettings;
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
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.yaml.snakeyaml.Yaml;

/**
 * The reference network architecture ({@code docs/security/NETWORK.md},
 * HIPAA.md §11.2 D1) names every port a stage listens on or connects to by
 * setting, and its network policies let in only those it listens on, so a
 * port added to the settings can't be left out of what the network
 * administrator opens.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class NetworkGuideTest
{

    /** The repository's root: the parent of the module's directory. */
    private static final Path ROOT = Path.of("").toAbsolutePath()
            .getParent();

    /** The fence that opens §4's network policies. */
    private static final String POLICIES = "```yaml network-policies\n";

    /** Creates the tests; JUnit runs each test on a new instance. */
    NetworkGuideTest()
    {
    }

    /**
     * Every port setting, nested ones by their full name, is a row of §2's
     * or §3's tables with its default, as {@code | `<name>` | <default> |}.
     *
     * @throws IOException if the document can't be read
     */
    @Test
    void namesEveryPortSetting() throws IOException
    {
        String guide = guide();
        Map<String, String> ports = ports();
        assertThat(ports).as("the port settings found").containsKeys(
                "sdh.landing.http.port", "sdh.landing.source.sftp.port",
                "sdh.stage.ready-port", "sdh.control.probe-port");
        Set<String> missing = new TreeSet<>();
        ports.forEach((setting, port) ->
        {
            if (!guide.contains("\n| `" + setting + "` | " + port + " |"))
            {
                missing.add(setting + " (" + port + ")");
            }
        });
        assertThat(missing).as("port settings NETWORK.md leaves out")
                .isEmpty();
    }

    /**
     * §4's policies are network policies, start with one that denies
     * everything, and let traffic in only on a port §2 says a stage listens
     * on.
     *
     * @throws IOException if the document can't be read
     */
    @Test
    void policiesLetInOnlyTheListeners() throws IOException
    {
        String guide = guide();
        int start = guide.indexOf(POLICIES);
        assertThat(start).as("§4's policies").isPositive();
        int end = guide.indexOf("\n```", start + POLICIES.length());
        String yaml = guide.substring(start + POLICIES.length(), end);
        List<Map<String, Object>> policies = new ArrayList<>();
        for (Object document : new Yaml().loadAll(yaml))
        {
            policies.add(map(document));
        }
        assertThat(policies).as("policies").isNotEmpty()
                .allSatisfy(policy -> assertThat(policy.get("kind"))
                        .isEqualTo("NetworkPolicy"));
        Map<String, Object> first = map(policies.get(0).get("spec"));
        assertThat(map(first.get("podSelector"))).as("default-deny's pods")
                .isEmpty();
        assertThat(first.get("policyTypes")).as("default-deny's types")
                .isEqualTo(List.of("Ingress", "Egress"));
        assertThat(first).as("default-deny's rules")
                .doesNotContainKeys("ingress", "egress");
        Set<Integer> listened = new TreeSet<>();
        Map<String, String> ports = ports();
        String listeners = guide.substring(guide.indexOf("## 2."),
                guide.indexOf("## 3."));
        ports.forEach((setting, port) ->
        {
            if (listeners.contains("| `" + setting + "` |"))
            {
                listened.add(Integer.valueOf(port));
            }
        });
        Set<Integer> admitted = new TreeSet<>();
        for (Map<String, Object> policy : policies)
        {
            Object ingress = map(policy.get("spec")).get("ingress");
            for (Object rule : ingress == null ? List.of() : (List<?>) ingress)
            {
                for (Object port : (List<?>) map(rule).get("ports"))
                {
                    admitted.add((Integer) map(port).get("port"));
                }
            }
        }
        assertThat(admitted).as("ports §4 lets in").isNotEmpty();
        assertThat(listened).as("ports §2 says are listened on")
                .containsAll(admitted);
    }

    /**
     * Reads the document.
     *
     * @return its text
     * @throws IOException if it can't be read
     */
    private static String guide() throws IOException
    {
        return Files.readString(ROOT.resolve("docs/security/NETWORK.md"));
    }

    /**
     * Every port setting of the landing node, the record stages and the
     * control API.
     *
     * @return their defaults, by setting
     */
    private static Map<String, String> ports()
    {
        Map<String, String> ports = new TreeMap<>();
        ports(LandingProperties.class, "sdh", ports);
        ports(StageProperties.class, StageProperties.PREFIX, ports);
        ports(ControlSettings.class, "sdh.control", ports);
        return ports;
    }

    /**
     * A YAML mapping, as parsed.
     *
     * @param value what was parsed
     * @return it as a map
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value)
    {
        assertThat(value).as("a mapping").isInstanceOf(Map.class);
        return (Map<String, Object>) value;
    }

    /**
     * Adds a record's port settings, with their defaults: each component
     * named {@code port} or ending in {@code Port}, and those of a nested
     * record under its name.
     *
     * @param type   the record
     * @param prefix its settings' prefix
     * @param ports  where they are added, by setting
     */
    private static void ports(Class<?> type, String prefix,
            Map<String, String> ports)
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
            RecordComponent component = components[i];
            String name = prefix + "." + kebab(component.getName());
            if (component.getType().isRecord())
            {
                ports(component.getType(), name, ports);
            }
            else if (component.getType() == int.class
                    && (component.getName().equals("port")
                            || component.getName().endsWith("Port")))
            {
                DefaultValue value = parameters[i]
                        .getAnnotation(DefaultValue.class);
                assertThat(value).as(name + "'s default").isNotNull();
                ports.put(name, value.value()[0]);
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
