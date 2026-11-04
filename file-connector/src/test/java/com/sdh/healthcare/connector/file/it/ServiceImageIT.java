/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * The service images (design §14.1): each application's {@code Dockerfile}
 * from its packaged jar. Full Ubuntu/Temurin images for our own hosted
 * service, with a shell (and, for the splitter, {@code sdh-admin}); not
 * distributed to customers.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ServiceImageIT extends ContainerImageItBase
{

    /**
     * {@inheritDoc}
     *
     * <p>The tag keeps Docker's layer cache between runs.
     */
    @Override
    ImageFromDockerfile image(String module) throws Exception
    {
        Path dir = module(module);
        ImageFromDockerfile image =
                new ImageFromDockerfile("sdh/" + module + "-service:it", false)
                        .withFileFromPath("Dockerfile",
                                dir.resolve("Dockerfile"))
                        .withFileFromPath("src/dist", dir.resolve("src/dist"));
        if (Files.isDirectory(dir.resolve("docker")))
        {
            image.withFileFromPath("docker", dir.resolve("docker"));
        }
        return image
                .withFileFromPath("app.jar", packagedJar(module))
                .withBuildArg("JAR_FILE", "app.jar")
                .withBuildArg("JRE_IMAGE", baseImage("TEMURIN_JRE_IMAGE"))
                .withBuildArg("JDK_IMAGE", baseImage("TEMURIN_JDK_IMAGE"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    boolean hasShell()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String admin(String... args) throws Exception
    {
        List<String> command = new ArrayList<>(
                List.of("env", "SDH_OPERATOR=it-operator", "sdh-admin"));
        command.addAll(List.of(args));
        return exec(app, command.toArray(String[]::new));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    String variant()
    {
        return "service";
    }

    /**
     * {@inheritDoc}
     *
     * <p>The Temurin JRE here isn't a Debian package; its notices are in
     * its own legal/ directory.
     */
    @Override
    void checkBaseImage(GenericContainer<?> container, List<String> packages)
            throws Exception
    {
        assertThat(read(container, "/opt/java/openjdk/legal/java.base/LICENSE"))
                .contains("GNU General Public License");
    }
}
