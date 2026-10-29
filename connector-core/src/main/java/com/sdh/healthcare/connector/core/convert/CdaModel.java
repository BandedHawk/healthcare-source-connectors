package com.sdh.healthcare.connector.core.convert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * The CDA R2 + SDTC element model: for each complex type, its child elements, their types and whether the schema
 * lets them repeat, plus whether the type carries text. Generated from HL7's schemas by
 * {@code scripts/cda-model/generate_cda_model.py} into {@code cda-r2-sdtc.model}.
 */
final class CdaModel {
    record Child(String type, boolean repeats) {}

    /** How an attribute's value is written: as a JSON string unless the schema types it boolean or integer. */
    enum Kind { BOOLEAN, INTEGER }

    record Type(boolean mixed, boolean text, Map<String, Child> children, Map<String, Kind> attributes) {}

    static final CdaModel INSTANCE = load();

    final String rootElement;
    final String rootType;
    private final Map<String, Type> types;

    private CdaModel(String rootElement, String rootType, Map<String, Type> types) {
        this.rootElement = rootElement;
        this.rootType = rootType;
        this.types = types;
    }

    /** The named type, or null if the model doesn't know it. */
    Type type(String name) {
        return name == null ? null : types.get(name);
    }

    private static CdaModel load() {
        String root = null;
        String rootType = null;
        Map<String, boolean[]> flags = new HashMap<>();
        Map<String, Map<String, Child>> children = new HashMap<>();
        Map<String, Map<String, Kind>> attributes = new HashMap<>();
        try (InputStream in = CdaModel.class.getResourceAsStream("cda-r2-sdtc.model")) {
            if (in == null) {
                throw new IllegalStateException("cda-r2-sdtc.model missing from the classpath");
            }
            BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = r.readLine()) != null) {
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] f = line.split("\t");
                switch (f[0]) {
                    case "root" -> {
                        root = f[1];
                        rootType = f[2];
                    }
                    case "type" -> flags.put(f[1], new boolean[] {f[2].equals("1"), f[3].equals("1")});
                    case "child" -> children.computeIfAbsent(f[1], k -> new HashMap<>())
                            .put(f[2], new Child(f[3], f[4].equals("1")));
                    case "attr" -> attributes.computeIfAbsent(f[1], k -> new HashMap<>())
                            .put(f[2], Kind.valueOf(f[3].toUpperCase(java.util.Locale.ROOT)));
                    default -> throw new IllegalStateException("Unknown model line type " + f[0]);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Map<String, Type> types = new HashMap<>();
        flags.forEach((name, fl) -> types.put(name, new Type(fl[0], fl[1],
                Map.copyOf(children.getOrDefault(name, Map.of())),
                Map.copyOf(attributes.getOrDefault(name, Map.of())))));
        return new CdaModel(root, rootType, Map.copyOf(types));
    }
}
