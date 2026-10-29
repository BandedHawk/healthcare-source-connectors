package com.sdh.healthcare.connector.core.convert;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Section converters by format (design §6.4), found with {@link ServiceLoader} like the splitters, so a plugin jar
 * can add one. Built in: {@code csv}, {@code ccda}, {@code x12}, {@code edifact}. Formats without a converter are
 * stored as split.
 */
public final class ConverterRegistry {
    private final Map<String, SectionConverter> byFormat;

    private ConverterRegistry(Map<String, SectionConverter> byFormat) {
        this.byFormat = Map.copyOf(byFormat);
    }

    public static ConverterRegistry load() {
        return load(ConverterRegistry.class.getClassLoader());
    }

    public static ConverterRegistry load(ClassLoader cl) {
        Map<String, SectionConverter> m = new HashMap<>();
        for (SectionConverter c : ServiceLoader.load(SectionConverter.class, cl)) {
            if (m.putIfAbsent(c.format(), c) != null) {
                throw new IllegalStateException("Two converters for format " + c.format());
            }
        }
        return new ConverterRegistry(m);
    }

    public static ConverterRegistry of(SectionConverter... converters) {
        Map<String, SectionConverter> m = new HashMap<>();
        for (SectionConverter c : converters) {
            m.put(c.format(), c);
        }
        return new ConverterRegistry(m);
    }

    /** No conversion at all: every section is stored as split. */
    public static ConverterRegistry none() {
        return new ConverterRegistry(Map.of());
    }

    public Optional<SectionConverter> get(String format) {
        return Optional.ofNullable(byFormat.get(format));
    }
}
