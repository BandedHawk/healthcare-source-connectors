# File connector container image, built from this distribution (design §14.3). Run from the distribution's top
# directory:
#
#   docker build -f container/Dockerfile -t <registry>/file-connector:<version> .
#   docker build -f container/Dockerfile --build-arg JAVA_IMAGE=<your Java 25 image> -t ... .
#
# JAVA_IMAGE is any Linux Java 25 runtime image with java on the PATH and glibc: for example
# gcr.io/distroless/java25-debian13:nonroot (the default; no shell or package tools) or eclipse-temurin:25-jre.
# Pin it by digest for production. The build runs no commands inside the image, so shell-less bases work.
#
# Run it read-only, with /tmp (noexec is fine), /data/inbound and the secrets mounted as volumes: see README.md.
# Operator commands: kubectl exec <pod> -- java @/app/jvm.options @/app/container.options @/app/admin.args status \
#   --operator alice

ARG JAVA_IMAGE=gcr.io/distroless/java25-debian13:nonroot
FROM ${JAVA_IMAGE}
# Set by BuildKit for the target platform; the classic builder has no default, so amd64 is assumed.
ARG TARGETARCH=amd64

LABEL org.opencontainers.image.title="file-connector" \
      org.opencontainers.image.description="Splits inbound healthcare files into encrypted Iceberg sections and announces them" \
      org.opencontainers.image.vendor="Smile Digital Health Inc." \
      org.opencontainers.image.licenses="LicenseRef-Proprietary" \
      org.opencontainers.image.documentation="Third-party notices: /app/legal/NOTICE and /app/legal/THIRD-PARTY.txt"

WORKDIR /app
COPY app.jar config/jvm.options container/container.options container/admin.args /app/
COPY native/linux-${TARGETARCH}/ /app/native/
COPY legal/ /app/legal/
# The inbound mount point, owned by the app user (normally replaced by a volume).
COPY --chown=10001:10001 container/inbound/ /data/inbound/

# A fixed non-root uid/gid, matching the Kubernetes securityContext; no account entry is needed.
USER 10001:10001
ENV TZ=UTC
ENTRYPOINT ["java", "@/app/jvm.options", "@/app/container.options", "-jar", "/app/app.jar"]
