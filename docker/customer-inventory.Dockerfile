# Adds the OS package inventory, /app/legal/os-packages.txt, to the customer image (design §14.2, §14.3): every
# package the base image brings, with its version, source package and copyright file, which is the record a source
# offer needs. Built by scripts/build-image.sh customer on top of the image from the distribution's
# container/Dockerfile. Our build tooling only; it isn't part of the distribution.
#
# Works for Debian-based images: package records in /var/lib/dpkg/status.d/ (distroless) or /var/lib/dpkg/status.
# The busybox stage only reads the records; nothing from it ends up in the image.
ARG KIT_IMAGE=sdh/file-connector:kit
FROM ${KIT_IMAGE} AS kit

FROM busybox:1.37.0 AS inventory
COPY --from=kit /var/lib/dpkg/ /dpkg/
COPY --from=kit /usr/share/doc/ /doc/
RUN set -eu; \
    { if [ -d /dpkg/status.d ]; then for f in /dpkg/status.d/*; do case "$f" in *.md5sums) ;; *) cat "$f"; echo; esac; done; \
      else cat /dpkg/status; fi; } \
    | awk 'BEGIN { RS = "" } \
           /(^|\n)Status: [^\n]*not-installed/ { next } \
           { p = v = s = sv = ""; n = split($0, l, "\n"); \
             for (i = 1; i <= n; i++) { \
               if (l[i] ~ /^Package: /) p = substr(l[i], 10); \
               else if (l[i] ~ /^Version: /) v = substr(l[i], 10); \
               else if (l[i] ~ /^Source: /) { split(substr(l[i], 9), a, " "); s = a[1]; if (a[2] ~ /^\(/) { sv = a[2]; gsub(/[()]/, "", sv) } } } \
             if (p == "") next; if (s == "") s = p; if (sv == "") sv = v; print p, v, s, sv }' \
    | sort > /packages; \
    { echo "# Operating system packages in this image: package, version, source package, source version, copyright file."; \
      while read -r p v s sv; do \
        c=$(readlink -f "/doc/$p/copyright" || true); \
        echo "$p $v $s $sv ${c:+/usr/share/doc${c#/doc}}"; \
      done < /packages; } > /os-packages.txt; \
    test "$(wc -l < /packages)" -gt 0

FROM kit
COPY --from=inventory /os-packages.txt /app/legal/os-packages.txt
