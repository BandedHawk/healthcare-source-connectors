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
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * The customer image (design §14.3), built as scripts/build-image.sh builds it: the distribution kit's
 * {@code container/Dockerfile} on the distroless Java 25 base, then {@code docker/customer-inventory.Dockerfile}
 * adds the OS package inventory. No shell, so operator commands run the JVM directly and files are read by copying
 * them out.
 */
class CustomerImageIT extends ContainerImageItBase {
    static final String KIT_IMAGE = "sdh/file-connector:it-kit";

    @Override
    ImageFromDockerfile image() throws Exception {
        Path kit;
        try (Stream<Path> s = Files.list(Path.of("target"))) {
            Path dir = s.filter(p -> p.getFileName().toString().endsWith("-dist-dir")).findFirst()
                    .orElseThrow(() -> new IllegalStateException("Package file-connector first (mvn -Pit verify)"));
            try (Stream<Path> inner = Files.list(dir)) {
                kit = inner.findFirst().orElseThrow();
            }
        }
        // The kit's own Dockerfile with the kit as the build context, as `docker build -f container/Dockerfile .`
        // would use them.
        ImageFromDockerfile fromKit = new ImageFromDockerfile(KIT_IMAGE, false)
                .withFileFromPath("Dockerfile", kit.resolve("container/Dockerfile"))
                .withBuildArg("JAVA_IMAGE", baseImage("DISTROLESS_IMAGE"));
        try (Stream<Path> entries = Files.list(kit)) {
            for (Path e : entries.toList()) {
                fromKit.withFileFromPath(e.getFileName().toString(), e);
            }
        }
        fromKit.get();
        return new ImageFromDockerfile("sdh/file-connector:it", false)
                .withFileFromPath("Dockerfile", Path.of("docker", "customer-inventory.Dockerfile"))
                .withBuildArg("KIT_IMAGE", KIT_IMAGE);
    }

    @Override
    boolean hasShell() {
        return false;
    }

    /** As the README documents: the JVM with the image's argument files, SDH_OPERATOR set for the exec. */
    @Override
    String admin(String... args) throws Exception {
        assertThat(app.isRunning()).as("connector still running; its log:\n%s", APP_LOG.toUtf8String()).isTrue();
        List<String> cmd = new ArrayList<>(List.of("java", "@/app/jvm.options", "@/app/container.options",
                "@/app/admin.args"));
        cmd.addAll(List.of(args));
        ExecResult r = app.execInContainer(UTF_8, ExecConfig.builder().command(cmd.toArray(String[]::new))
                .envVars(Map.of("SDH_OPERATOR", "it-operator")).build());
        return r.getExitCode() + ":" + r.getStdout().strip() + r.getStderr().strip();
    }

    @Override
    String variant() {
        return "customer";
    }

    /**
     * Only what Java needs: the JRE as a Debian package, the C library and a few data and font libraries. No shell,
     * core utilities or package tools, which is most of the GPL userland a full base image brings.
     */
    @Override
    void checkBaseImage(List<String> packages) {
        assertThat(packages).contains("temurin-25-jre", "libc6")
                .doesNotContain("bash", "dash", "busybox", "coreutils", "findutils", "grep", "sed", "tar", "gzip",
                        "dpkg", "apt", "util-linux", "login", "passwd");
        assertThat(packages).hasSizeLessThan(40);
    }
}
