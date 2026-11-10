# Build kit

The build files of `healthcare-source-connectors` that a project built on it shares (STAGES.md §17.10.10, point 1),
so every stage's kit and images are built to the same rules:

- `src/license/`: the licence policy (`licenses.xml`), the overrides, copyrights and bundling notes, and the
  attribution template the root `pom.xml`'s licence and attribution steps read;
- `docker/`: the image bases pinned by digest (`base-images.env`), the accepted vulnerabilities and the inventory
  Dockerfile every customer image ends with;
- `scripts/`: `build-image.sh`, and the minikube scripts it runs in the `sdh-it` VM with.

## Using it

The project takes `com.sdh.healthcare:healthcare-source-connectors` as its parent, unpacks this zip
(`com.sdh.healthcare:connector-build-kit:<version>:zip`) into its own `target/build-kit` before packaging, and sets
the two properties the parent's paths are made of:

```xml
<sdh.license.dir>${maven.multiModuleProjectDirectory}/target/build-kit/src/license</sdh.license.dir>
<sdh.build-image>${maven.multiModuleProjectDirectory}/target/build-kit/scripts/build-image.sh</sdh.build-image>
```

`build-image.sh <module folder> customer|service` then builds a module's image with the kit's `docker/` and the
project's own Maven wrapper (the nearest `mvnw` above the module). The project keeps its own `LICENSE` and `NOTICE`
at its root, which the parent puts into every jar.

Change these files in `healthcare-source-connectors`, never in a copy: a project picks a change up with the kit's
next version.
