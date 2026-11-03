# landing-connector SERVICE image: for our own hosted service only, NOT for customer distribution (design §14.1,
# §14.3). It is a full Ubuntu/Temurin image with a shell. The image distributed to customers is built from the
# distribution kit instead (container/Dockerfile in target/landing-connector-*-dist-dir; scripts/build-image.sh
# landing customer). Build the jar first, then build from this directory:
#
#   ./mvnw -pl landing-connector -am package -DskipTests
#   docker build -t sdh/landing-connector-service:0.1.0-SNAPSHOT landing-connector
#
# or use scripts/build-image.sh landing, which does both against the minikube VM's Docker daemon.
#
# Run it as deployed: read-only root filesystem, a writable /tmp (noexec is fine), the intake directory and
# secrets mounted as volumes, all capabilities dropped. The image itself runs as uid/gid 10001.
#
# scripts/build-image.sh passes both base images pinned by digest from docker/base-images.env (design §14.5); the
# defaults below are only for a plain `docker build`.

ARG JDK_IMAGE=eclipse-temurin:25-jdk-noble
ARG JRE_IMAGE=eclipse-temurin:25-jre-noble

FROM ${JDK_IMAGE} AS extract
ARG JAR_FILE=target/landing-connector-*.jar
ARG TARGETARCH
WORKDIR /build
COPY ${JAR_FILE} app.jar
# Spring Boot layers, least to most frequently changed, so rebuilds reuse the dependency layer.
RUN java -Djarmode=tools -jar app.jar extract --layers --destination layers
# Third-party attribution (design §14.2), generated into the jar at build time: LICENSE, NOTICE, THIRD-PARTY.txt and
# the license texts. Put where they can be read without opening the jar.
RUN mkdir legal && cd legal \
 && jar xf ../app.jar META-INF/LICENSE META-INF/NOTICE META-INF/THIRD-PARTY.txt META-INF/licenses \
 && mv META-INF/* . && rmdir META-INF \
 && test -s THIRD-PARTY.txt && test -s NOTICE
# zstd-jni (section and Parquet compression) and snappy-java (an optional Kafka codec, probed at startup) normally
# unpack their native libraries into java.io.tmpdir and load them from there, which fails on a noexec /tmp. Unpack
# them here instead; the -D options in ENTRYPOINT point both libraries at /app/native.
RUN set -eu; \
    case "${TARGETARCH:-$(dpkg --print-architecture)}" in \
      amd64) zstd=amd64; snappy=x86_64 ;; \
      arm64) zstd=aarch64; snappy=aarch64 ;; \
      *) echo "unsupported architecture" >&2; exit 1 ;; \
    esac; \
    mkdir -p native unpack; \
    cd unpack; \
    jar xf "$(ls ../layers/dependencies/lib/zstd-jni-*.jar)" "linux/$zstd/"; \
    jar xf "$(ls ../layers/dependencies/lib/snappy-java-*.jar)" "org/xerial/snappy/native/Linux/$snappy/"; \
    cp linux/$zstd/libzstd-jni-*.so ../native/libzstd-jni.so; \
    cp org/xerial/snappy/native/Linux/$snappy/libsnappyjava.so ../native/libsnappyjava.so

FROM ${JRE_IMAGE}
LABEL org.opencontainers.image.title="landing-connector (service)" \
      org.opencontainers.image.description="Lands healthcare data from an intake, encrypted, in Iceberg and announces each item" \
      org.opencontainers.image.vendor="Smile Digital Health Inc." \
      org.opencontainers.image.licenses="LicenseRef-Proprietary" \
      org.opencontainers.image.documentation="Third-party notices: /app/legal/NOTICE and /app/legal/THIRD-PARTY.txt"

# No home directory or login shell. The app owns nothing in the image except the intake mount point.
RUN groupadd --system --gid 10001 sdh \
 && useradd --system --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin sdh \
 && mkdir -p /data/intake /run/secrets \
 && chown 10001:10001 /data/intake \
 && chmod 0750 /data/intake

WORKDIR /app
COPY --from=extract /build/layers/dependencies/ ./
COPY --from=extract /build/layers/spring-boot-loader/ ./
COPY --from=extract /build/layers/snapshot-dependencies/ ./
COPY --from=extract /build/layers/application/ ./
COPY --from=extract /build/native/ ./native/
COPY --from=extract /build/legal/ ./legal/

# The same JVM options as the customer image and the distribution (src/dist).
COPY src/dist/config/jvm.options src/dist/container/container.options /app/
RUN chmod 0644 /app/jvm.options /app/container.options
# Every OS package in this image, with the source package it was built from (design §14.2): the attribution and
# source-availability record for what the base image brings. Each package's terms are in
# /usr/share/doc/<package>/copyright; the Java runtime's are in /opt/java/openjdk/legal/.
RUN { echo "# Operating system packages in this image: package, version, source package, source version, copyright file."; \
      dpkg-query -W -f='${Package} ${Version} ${source:Package} ${source:Version}\n' | sort \
      | while read -r p v sp sv; do echo "$p $v $sp $sv $(readlink -f "/usr/share/doc/${p%%:*}/copyright")"; done; } \
      > /app/legal/os-packages.txt \
 && chmod 0644 /app/legal/os-packages.txt

USER 10001:10001
ENV TZ=UTC

# JVM options: the PHI hygiene flags (design §7.5) in /app/jvm.options, container settings in /app/container.options.
# Arguments after the image name go to the app, e.g. `--sdh.landing.standby=true`.
ENTRYPOINT ["java", "@/app/jvm.options", "@/app/container.options", "-jar", "/app/app.jar"]
