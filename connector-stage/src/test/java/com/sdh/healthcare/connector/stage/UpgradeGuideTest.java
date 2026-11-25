/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.contract.ContractVersion;
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
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

/**
 * Upgrades and patching ({@code docs/security/UPGRADES.md}, HIPAA.md §11.2
 * D12) says how a release is taken without losing data, and it stays true
 * of the code: every way the drain check lets a release in is named, with
 * the contract version written and the drain check's setting; every stage
 * Deployment in the kits' READMEs adds a pod before it takes one away; and
 * no stage's code drops, renames or retypes a table or column, so the old
 * pods, through a rollout, and the release before, after a rollback, read
 * the tables a release makes.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class UpgradeGuideTest
{

    /** The repository's root: the parent of the module's directory. */
    private static final Path ROOT = Path.of("").toAbsolutePath()
            .getParent();

    /**
     * The transform stage's repository, when it is checked out beside this
     * one.
     */
    private static final Path TRANSFORMER = ROOT.resolveSibling(
            "healthcare-data-transformer");

    /** The kits whose workers the drain check holds. */
    private static final List<Path> STAGE_KITS = List.of(
            ROOT.resolve("file-connector/src/dist/README.md"),
            ROOT.resolve("convert-stage/src/dist/README.md"),
            TRANSFORMER.resolve("transform-stage/src/dist/README.md"));

    /** A README's YAML blocks, whatever their fence's label. */
    private static final Pattern YAML = Pattern.compile(
            "```yaml[^\\n]*\\n(.*?)```", Pattern.DOTALL);

    /**
     * What a release must not do to a table, as SQL in a string or as an
     * Iceberg schema update: anything but add.
     */
    private static final Pattern NOT_ADDING = Pattern.compile(
            "DROP\\s+(?:COLUMN|TABLE|SCHEMA)\\b|\\bRENAME\\s+(?:COLUMN|TO)\\b"
                    + "|\\bSET\\s+DATA\\s+TYPE\\b|ALTER\\s+COLUMN\\s+\\w+"
                    + "\\s+TYPE\\b|\\bTRUNCATE\\b|\\.deleteColumn\\("
                    + "|\\.renameColumn\\(|\\.updateColumn\\(");

    /**
     * Modules whose code isn't a stage's: the control harness drops the
     * throwaway databases it made.
     */
    private static final Set<String> NOT_A_STAGE = Set.of("control-harness");

    /** Creates the tests; JUnit runs each test on a new instance. */
    UpgradeGuideTest()
    {
    }

    /**
     * Every way the drain check lets a release in is named in code quotes,
     * as it is audited, with the contract version this build writes and
     * the drain check's setting with its default as a row.
     *
     * @throws IOException if the document can't be read
     */
    @Test
    void namesEveryWayAReleaseIsLetIn() throws IOException
    {
        String guide = guide();
        for (DrainGate.How how : DrainGate.How.values())
        {
            assertThat(guide).as("how a release is let in")
                    .contains("`" + how.wireName() + "`");
        }
        assertThat(guide).as("the contract version written")
                .contains("writes `" + ContractVersion.CURRENT + "`");
        assertThat(guide).as("the drain check's setting")
                .contains("\n| `" + StageProperties.PREFIX
                        + ".version-check-interval` | 1m |");
    }

    /**
     * Every worker Deployment of the split, convert and transform stages
     * in the kits' READMEs is a rolling update that adds a pod first and
     * takes none away, as §3 has it; the control Deployments aren't held by
     * the drain check and are left out.
     *
     * @throws IOException if a README can't be read
     */
    @Test
    void everyStageDeploymentAddsAPodFirst() throws IOException
    {
        assertThat(guide()).as("§3's strategy").contains(
                "`rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }`");
        List<String> checked = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        for (Path kit : STAGE_KITS)
        {
            if (!Files.isRegularFile(kit))
            {
                continue;
            }
            Matcher block = YAML.matcher(Files.readString(kit));
            while (block.find())
            {
                for (Object document : new Yaml().loadAll(block.group(1)))
                {
                    if (document instanceof Map<?, ?> resource
                            && "Deployment".equals(resource.get("kind")))
                    {
                        String name = String.valueOf(map(resource.get(
                                "metadata")).get("name"));
                        if (name.startsWith("sdh-control-"))
                        {
                            continue;
                        }
                        checked.add(name);
                        Map<String, Object> strategy = map(map(resource
                                .get("spec")).get("strategy"));
                        Map<String, Object> rolling = map(strategy.get(
                                "rollingUpdate"));
                        if (!"RollingUpdate".equals(strategy.get("type"))
                                || !Integer.valueOf(1).equals(
                                        rolling.get("maxSurge"))
                                || !Integer.valueOf(0).equals(
                                        rolling.get("maxUnavailable")))
                        {
                            problems.add(kit + ": " + name);
                        }
                    }
                }
            }
        }
        assertThat(checked).as("the stages' worker Deployments")
                .hasSizeGreaterThanOrEqualTo(2);
        assertThat(problems).as("Deployments that don't add a pod first")
                .isEmpty();
    }

    /**
     * No stage's main code drops, renames or retypes a table or column, or
     * truncates one: a release only adds to its tables.
     *
     * @throws IOException if a file can't be read
     */
    @Test
    void releasesOnlyAddToTheirTables() throws IOException
    {
        Set<String> found = new TreeSet<>();
        for (Path source : sources())
        {
            Matcher change = NOT_ADDING.matcher(Files.readString(source));
            while (change.find())
            {
                found.add(source.getFileName() + ": " + change.group());
            }
        }
        assertThat(found).as("table changes that don't only add").isEmpty();
    }

    /**
     * Reads the document.
     *
     * @return its text
     * @throws IOException if it can't be read
     */
    private static String guide() throws IOException
    {
        return Files.readString(ROOT.resolve("docs/security/UPGRADES.md"));
    }

    /**
     * The stages' main Java sources: this repository's, and the transform
     * stage's when its repository is checked out beside this one.
     *
     * @return their paths
     * @throws IOException if a tree can't be walked
     */
    private static List<Path> sources() throws IOException
    {
        List<Path> sources = new ArrayList<>();
        for (Path root : List.of(ROOT, TRANSFORMER))
        {
            if (!Files.isDirectory(root))
            {
                continue;
            }
            try (Stream<Path> walk = Files.walk(root))
            {
                walk.filter(path -> path.toString().endsWith(".java")
                        && path.toString().contains("/src/main/java/")
                        && !NOT_A_STAGE.contains(root.relativize(path)
                                .getName(0).toString()))
                        .forEach(sources::add);
            }
        }
        return sources;
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
}
