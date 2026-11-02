/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import java.util.Map;

/**
 * What the stored bytes of a section are (design §6.4): JSON made by a
 * converter, or the section as split, in its original format. It is stored
 * with every section, in the {@code content_type} and {@code mapping} columns,
 * and carried in the message announcing the section, so whatever reads the
 * section knows how to read it.
 * <p>
 * Create one with {@link #json} for a converted section, or with {@link #raw}
 * for a section stored as split: one in a format without a converter, or one
 * whose conversion failed.
 *
 * @param contentType the media type of the stored bytes:
 *                    {@value #JSON} for a converted section, otherwise the
 *                    format's own, such as {@code text/csv}
 * @param mapping     the name and version of the converter's JSON, such as
 *                    {@code "csv-row/1"} ({@link SectionConverter#mapping()});
 *                    {@code null} for a section stored as split
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record Representation(String contentType, String mapping)
{

    /** The media type of a converted section: JSON. */
    public static final String JSON = "application/json";

    /**
     * The media type of a section stored as split, by the name of its format.
     * A format not listed here, such as {@code delimited} or one added as a
     * plugin, is stored as {@code application/octet-stream}: bytes of no
     * stated type.
     */
    private static final Map<String, String> RAW = Map.of(
            "csv", "text/csv",
            "ccda", "application/xml",
            "xml", "application/xml",
            "x12", "application/edi-x12",
            "edifact", "application/edifact",
            "fhir", "application/fhir+json");

    /**
     * The representation of a converted section.
     *
     * @param mapping the name and version of the converter's JSON, from
     *                {@link SectionConverter#mapping()}
     * @return a representation with the media type {@value #JSON} and that
     *         mapping
     */
    public static Representation json(String mapping)
    {
        return new Representation(JSON, mapping);
    }

    /**
     * The representation of a section stored as split, in its original
     * format.
     *
     * @param format the name of the section's format, such as {@code "csv"}
     * @return a representation with the format's media type, or
     *         {@code application/octet-stream} for a format with none listed,
     *         and no mapping
     */
    public static Representation raw(String format)
    {
        return new Representation(RAW.getOrDefault(format,
                "application/octet-stream"), null);
    }
}
