/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
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
 * What HL7's CDA schema says about each kind of element, in the form the C-CDA
 * converter needs to write a document as JSON (design §6.4).
 * <p>
 * The schema gives every element a type, such as {@code POCD_MT000040.Patient}
 * or {@code PQ} (a physical quantity). For each type, the model records:
 * <ul>
 * <li>which child elements it can have, what type each child is, and whether
 * the schema lets that child occur more than once;</li>
 * <li>which of its attributes are booleans or whole numbers (all others are
 * strings);</li>
 * <li>whether the element can hold text, and whether that text can be mixed in
 * among child elements, as in a section's narrative.</li>
 * </ul>
 * The converter starts at the document's root element, whose type the model
 * also records, and looks up each child's type in its parent's type as it reads
 * further in. The answers decide the shape of the JSON: a child the schema lets
 * repeat is always an array, even with one occurrence, so a document's JSON has
 * the same shape however many of each element it happens to have.
 * <p>
 * The model covers CDA Release 2 with HL7's SDTC extensions, the extra elements
 * and attributes US realm documents use (such as {@code sdtc:raceCode}). SDTC
 * names carry an {@code sdtc:} prefix, as they do in the JSON.
 * <p>
 * It isn't read from the schema at run time. The script
 * {@code scripts/cda-model/generate_cda_model.py} reads HL7's schema files once
 * and writes the result to {@code cda-r2-sdtc.model}, a small text file on the
 * class path that this class loads. To move to a new schema version, re-run the
 * script and commit the new file.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 * @see CcdaConverter
 */
final class CdaModel
{

    /**
     * One child element a type can have, as the schema declares it.
     *
     * @param type the child's type, the name to look it up by with
     * {@link CdaModel#type}; it may be one the model doesn't describe (two are:
     * {@code StrucDoc.Br}, a line break in the narrative, and {@code list_int},
     * a list of numbers)
     * @param repeats whether the schema lets the child occur more than once
     * (its {@code maxOccurs} is above 1, or it sits in a group that repeats),
     * in which case the converter always writes it as a JSON array
     */
    record Child(String type, boolean repeats)
    {

    }

    /**
     * How an attribute's value is written in the JSON. Attributes the schema
     * types as a boolean or a whole number are listed in
     * {@link Type#attributes} with one of these; every other attribute is
     * written as a string, exactly as in the document.
     */
    enum Kind
    {
        /**
         * A boolean, such as {@code BL}'s {@code value} or {@code AD}'s
         * {@code isNotOrdered}: {@code "true"} or {@code "1"} becomes JSON
         * {@code true}, and {@code "false"} or {@code "0"} becomes
         * {@code false}.
         */
        BOOLEAN,
        /**
         * A whole number, such as {@code INT}'s {@code value}: written as a
         * JSON number when it is one.
         */
        INTEGER
    }

    /**
     * What the schema says about one type.
     *
     * @param mixed whether text can appear between the type's child elements,
     * as in a narrative block, an address or a name; when such an element has
     * attributes or children, the converter lists its separate pieces of text
     * rather than joining them into one
     * @param text whether elements of this type can hold text at all; always
     * {@code true} when {@code mixed} is (and, in the current schema, only
     * then). For a type that holds none, such as a {@code patient} element's,
     * the whitespace that indents its children is not written
     * @param children the child elements the type can have, by the name each
     * has in the JSON; empty for a type with none
     * @param attributes the type's boolean and whole-number attributes, by
     * name; attributes not in it are strings
     */
    record Type(boolean mixed, boolean text, Map<String, Child> children,
            Map<String, Kind> attributes)
    {

    }

    /**
     * The model, loaded from {@code cda-r2-sdtc.model} the first time this
     * class is used. There is only ever one, shared by every conversion; it
     * never changes once loaded, so it is safe to use from several threads at
     * once.
     */
    static final CdaModel INSTANCE = load();

    /**
     * The name of the element a CDA document starts with:
     * {@code ClinicalDocument}.
     */
    final String rootElement;
    /**
     * The type of the {@link #rootElement}, where the converter starts looking
     * up types: {@code POCD_MT000040.ClinicalDocument}.
     */
    final String rootType;
    /**
     * Every type the model describes, by name.
     */
    private final Map<String, Type> types;

    /**
     * Creates the model from what {@link #load} read. Only {@code load} calls
     * this; everything else uses {@link #INSTANCE}.
     *
     * @param rootElement the name of the document's root element
     * @param rootType the root element's type
     * @param types every type the model describes, by name; not copied, so it
     * must not change afterwards
     */
    private CdaModel(String rootElement, String rootType,
            Map<String, Type> types)
    {
        this.rootElement = rootElement;
        this.rootType = rootType;
        this.types = types;
    }

    /**
     * Looks up a type by name.
     * <p>
     * The converter treats {@code null} as "unknown" and plays safe: the
     * element's text is kept, and each of its children is written as an array.
     * That happens for an element the schema doesn't declare (a vendor's
     * extension, say), a misspelt {@code xsi:type}, and the two child types the
     * model doesn't describe ({@link Child#type}).
     *
     * @param name the type's name, as given by {@link Child#type},
     *             {@link #rootType} or an element's {@code xsi:type}; may be
     *             {@code null}
     * @return what the schema says about the type, or {@code null} if the name
     * is {@code null} or the model doesn't describe that type
     */
    Type type(String name)
    {
        return name == null ? null : types.get(name);
    }

    /**
     * Reads {@code cda-r2-sdtc.model} from the class path and builds the model
     * from it.
     * <p>
     * The file is UTF-8 text with one fact per line, its fields separated by
     * tabs. Empty lines and lines starting with {@code #} are comments. The
     * first field says what the line describes:
     * <ul>
     * <li>{@code root}, element, type: the document's root element and its
     * type;</li>
     * <li>{@code type}, type, mixed, text: a type, with {@code 1} or {@code 0}
     * for whether its text is mixed with child elements and whether it holds
     * text;</li>
     * <li>{@code child}, type, element, child type, repeats: one child element
     * of a type, with {@code 1} or {@code 0} for whether it can repeat;</li>
     * <li>{@code attr}, type, attribute, {@code boolean} or {@code integer}: an
     * attribute that isn't a string.</li>
     * </ul>
     * Children and attributes are only kept for types that have a {@code type}
     * line.
     *
     * @return the model, which can't be changed
     * @throws IllegalStateException if the file is missing from the class path
     * or has a line of a kind not listed above (a line with missing fields
     * fails too, with an unchecked exception of its own); as {@link #INSTANCE}
     * is set when the class is first used, this stops the converter from being
     * used at all, rather than letting it write JSON of the wrong shape
     * @throws UncheckedIOException if the file can't be read
     */
    private static CdaModel load()
    {
        // From the "root" line.
        String root = null;
        String rootType = null;
        // From the "type" lines: each type's mixed and text flags, in that
        // order.
        Map<String, boolean[]> flags = new HashMap<>();
        // From the "child" lines: each type's children, by element name.
        Map<String, Map<String, Child>> children = new HashMap<>();
        // From the "attr" lines: each type's non-string attributes, by name.
        Map<String, Map<String, Kind>> attributes = new HashMap<>();
        try (InputStream stream = CdaModel.class.getResourceAsStream(
                "cda-r2-sdtc.model"))
        {
            if (stream == null)
            {
                throw new IllegalStateException(
                        "cda-r2-sdtc.model missing from the classpath");
            }
            var reader = new BufferedReader(new InputStreamReader(stream,
                    StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null)
            {
                if (line.isEmpty() || line.startsWith("#"))
                {
                    continue;
                }
                // The line's tab-separated fields; field[0] says what it
                // describes.
                String[] field = line.split("\t");
                switch (field[0])
                {
                    case "root" ->
                    {
                        root = field[1];
                        rootType = field[2];
                    }
                    case "type" ->
                        flags.put(field[1], new boolean[]
                        {
                            field[2].equals("1"), field[3].equals("1")
                        });
                    case "child" ->
                        children.computeIfAbsent(field[1], k -> new HashMap<>())
                                .put(field[2], new Child(field[3], field[4].
                                        equals("1")));
                    case "attr" ->
                        attributes.computeIfAbsent(field[1], k
                                -> new HashMap<>())
                                .put(field[2], Kind.valueOf(field[3].
                                        toUpperCase(
                                                java.util.Locale.ROOT)));
                    default ->
                        throw new IllegalStateException(
                                "Unknown model line type " + field[0]);
                }
            }
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        // Put each type's flags, children and attributes together, in maps
        // that can't be changed.
        Map<String, Type> types = new HashMap<>();
        flags.forEach((name, flag) -> types.put(name, new Type(flag[0], flag[1],
                Map.copyOf(children.getOrDefault(name, Map.of())),
                Map.copyOf(attributes.getOrDefault(name, Map.of())))));
        return new CdaModel(root, rootType, Map.copyOf(types));
    }
}
