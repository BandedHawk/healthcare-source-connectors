package com.sdh.healthcare.connector.file.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * The service image (design §14.1): {@code file-connector/Dockerfile} from the packaged jar. A full Ubuntu/Temurin
 * image for our own hosted service, with a shell and {@code sdh-admin}; not distributed to customers.
 */
class ServiceImageIT extends ContainerImageItBase {

    /** The tag keeps Docker's layer cache between runs. */
    @Override
    ImageFromDockerfile image() throws Exception {
        return new ImageFromDockerfile("sdh/file-connector-service:it", false)
                .withFileFromPath("Dockerfile", Path.of("Dockerfile"))
                .withFileFromPath("docker", Path.of("docker"))
                .withFileFromPath("src/dist", Path.of("src/dist"))
                .withFileFromPath("app.jar", packagedJar())
                .withBuildArg("JAR_FILE", "app.jar")
                .withBuildArg("JRE_IMAGE", baseImage("TEMURIN_JRE_IMAGE"))
                .withBuildArg("JDK_IMAGE", baseImage("TEMURIN_JDK_IMAGE"));
    }

    @Override
    boolean hasShell() {
        return true;
    }

    @Override
    String admin(String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("env", "SDH_OPERATOR=it-operator", "sdh-admin"));
        cmd.addAll(List.of(args));
        return exec(cmd.toArray(String[]::new));
    }

    @Override
    String variant() {
        return "service";
    }

    /** The Temurin JRE here isn't a Debian package; its notices are in its own legal/ directory. */
    @Override
    void checkBaseImage(List<String> packages) throws Exception {
        assertThat(read("/opt/java/openjdk/legal/java.base/LICENSE")).contains("GNU General Public License");
    }
}
