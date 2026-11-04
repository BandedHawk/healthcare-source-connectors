/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.ExecConfig;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * The customer images (design §14.3), built as scripts/build-image.sh
 * builds them: each distribution kit's {@code container/Dockerfile} on the
 * distroless Java 25 base, then {@code docker/customer-inventory.Dockerfile}
 * adds the OS package inventory. No shell, so operator commands run the JVM
 * directly and files are read by copying them out.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class CustomerImageIT extends ContainerImageItBase
{

    /**
     * {@inheritDoc}
     */
    @Override
    ImageFromDockerfile image(String module) throws Exception
    {
        String kitImage = "sdh/" + module + ":it-kit";
        Path kit;
        try (Stream<Path> paths = Files.list(module(module).resolve("target")))
        {
            Path dir = paths.filter(path -> path.getFileName().toString()
                            .endsWith("-dist-dir"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Package "
                            + module + " first (mvn -Pit verify)"));
            try (Stream<Path> inner = Files.list(dir))
            {
                kit = inner.findFirst().orElseThrow();
            }
        }
        // The kit's own Dockerfile with the kit as the build context, as
        // `docker build -f container/Dockerfile .` would use them.
        ImageFromDockerfile fromKit = new ImageFromDockerfile(kitImage, false)
                .withFileFromPath("Dockerfile",
                        kit.resolve("container/Dockerfile"))
                .withBuildArg("JAVA_IMAGE", baseImage("DISTROLESS_IMAGE"));
        try (Stream<Path> entries = Files.list(kit))
        {
            for (Path entry : entries.toList())
            {
                fromKit.withFileFromPath(entry.getFileName().toString(),
                        entry);
            }
        }
        fromKit.get();
        return new ImageFromDockerfile("sdh/" + module + ":it", false)
                .withFileFromPath("Dockerfile", Path.of("..", "docker",
                        "customer-inventory.Dockerfile"))
                .withBuildArg("KIT_IMAGE", kitImage);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    boolean hasShell()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * <p>As the README documents: the JVM with the image's argument files,
     * SDH_OPERATOR set for the exec.
     */
    @Override
    String admin(String... args) throws Exception
    {
        assertThat(app.isRunning())
                .as("connector still running; its log:\n%s",
                        APP_LOG.toUtf8String())
                .isTrue();
        List<String> command = new ArrayList<>(List.of("java",
                "@/app/jvm.options", "@/app/container.options",
                "@/app/admin.args"));
        command.addAll(List.of(args));
        ExecResult result = app.execInContainer(UTF_8, ExecConfig.builder()
                .command(command.toArray(String[]::new))
                .envVars(Map.of("SDH_OPERATOR", "it-operator")).build());
        return result.getExitCode() + ":" + result.getStdout().strip()
                + result.getStderr().strip();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String variant()
    {
        return "customer";
    }

    /**
     * {@inheritDoc}
     *
     * <p>Only what Java needs: the JRE as a Debian package, the C library
     * and a few data and font libraries. No shell, core utilities or
     * package tools, which is most of the GPL userland a full base image
     * brings.
     */
    @Override
    void checkBaseImage(GenericContainer<?> container, List<String> packages)
    {
        assertThat(packages).contains("temurin-25-jre", "libc6")
                .doesNotContain("bash", "dash", "busybox", "coreutils",
                        "findutils", "grep", "sed", "tar", "gzip", "dpkg",
                        "apt", "util-linux", "login", "passwd");
        assertThat(packages).hasSizeLessThan(40);
    }
}
