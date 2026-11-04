/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * The section converters the connector can use, each found by the name of the
 * file format it converts (design §6.4).
 * <p>
 * After a file is split into sections, the pipeline asks the registry for the
 * converter of the file's format ({@link #get}), and uses it to turn each
 * section into JSON before storing it. A format with no converter, such as FHIR
 * (already JSON), generic XML or delimited text, is stored as split.
 * <p>
 * The connector itself builds its registry with {@link #load()}, or with
 * {@link #none()} when conversion is turned off ({@code sdh.split.convert:
 * false}). {@link #of} is for tests and for code that wants to choose its own
 * converters.
 * <p>
 * Converters are found the same way as splitters, with Java's
 * {@link ServiceLoader}: each jar lists its converters in the
 * {@code META-INF/services} file named after {@link SectionConverter}'s
 * full class name.
 * So a plugin jar can add a converter for a new format just by being on the
 * class path, without any change here. The connector's own jar lists four:
 * {@code csv}, {@code ccda}, {@code x12} and {@code edifact}.
 * <p>
 * A registry can't be changed once made, and its converters are thread-safe
 * ({@link SectionConverter}), so one registry can serve every file and worker
 * thread at once.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see SectionConverter
 * @see com.sdh.healthcare.connector.core.split.SplitterRegistry
 */
public final class ConverterRegistry
{

    /**
     * The converters, by the format name each gives in
     * {@link SectionConverter#format()}, such as {@code "ccda"}. At most one
     * converter per format. The map can't be changed.
     */
    private final Map<String, SectionConverter> byFormat;

    /**
     * Creates a registry of the given converters. Only this class's factory
     * methods call it.
     *
     * @param byFormat the converters by format name; copied, so later changes
     * to it don't affect the registry
     */
    private ConverterRegistry(Map<String, SectionConverter> byFormat)
    {
        this.byFormat = Map.copyOf(byFormat);
    }

    /**
     * Finds every converter on the class path that loaded this class: the
     * connector's own four, and any added by plugin jars. This is what the
     * connector uses when conversion is on.
     *
     * @return a registry of the converters found
     * @throws IllegalStateException if two converters give the same format
     * name, since it couldn't then tell which one to use
     * @throws java.util.ServiceConfigurationError if a listed converter can't
     * be created, for example because its class is missing or has no public
     * constructor without parameters
     * @see #load(ClassLoader)
     */
    public static ConverterRegistry load()
    {
        return load(ConverterRegistry.class.getClassLoader());
    }

    /**
     * Finds every converter that the given class loader can see, as
     * {@link #load()} does for the connector's own class loader. Useful when
     * plugins are loaded with a class loader of their own.
     *
     * @param classLoader the class loader to search for converters
     * @return a registry of the converters found
     * @throws IllegalStateException if two converters give the same format
     * name, since it couldn't then tell which one to use
     * @throws java.util.ServiceConfigurationError if a listed converter can't
     * be created
     */
    public static ConverterRegistry load(ClassLoader classLoader)
    {
        // The converters found so far, by format name.
        Map<String, SectionConverter> map = new HashMap<>();
        for (SectionConverter converter : ServiceLoader.load(
                SectionConverter.class, classLoader))
        {
            if (map.putIfAbsent(converter.format(), converter) != null)
            {
                throw new IllegalStateException(
                        "Two converters for format " + converter.format());
            }
        }
        return new ConverterRegistry(map);
    }

    /**
     * Makes a registry of exactly the converters given, without looking on the
     * class path. Tests use it to try the pipeline with a converter of their
     * own, such as one that fails on purpose.
     * <p>
     * Unlike {@link #load()}, it doesn't refuse two converters for the same
     * format: the one given last is kept.
     *
     * @param converters the converters to use; none at all gives the same as
     * {@link #none()}
     * @return a registry of those converters
     */
    public static ConverterRegistry of(SectionConverter... converters)
    {
        // The converters given, by format name; a later one replaces an
        // earlier one with the same name.
        Map<String, SectionConverter> m = new HashMap<>();
        for (SectionConverter converter : converters)
        {
            m.put(converter.format(), converter);
        }
        return new ConverterRegistry(m);
    }

    /**
     * Makes a registry with no converters, so every section is stored exactly
     * as it was split. The connector uses it when conversion is turned off
     * ({@code sdh.split.convert: false}).
     *
     * @return an empty registry
     */
    public static ConverterRegistry none()
    {
        return new ConverterRegistry(Map.of());
    }

    /**
     * Looks up the converter for a file format.
     *
     * @param format the format's name, as the connector is configured with,
     * such as {@code "ccda"}; it must match exactly, case included
     * @return the converter for that format, or an empty {@code Optional} if
     * there is none, in which case the format's sections are stored as split
     */
    public Optional<SectionConverter> get(String format)
    {
        return Optional.ofNullable(byFormat.get(format));
    }
}
