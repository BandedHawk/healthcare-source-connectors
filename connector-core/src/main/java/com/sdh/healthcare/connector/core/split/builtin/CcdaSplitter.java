/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

/**
 * {@link XmlSplitter} with C-CDA defaults: element {@code ClinicalDocument} in
 * namespace {@code urn:hl7-org:v3}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class CcdaSplitter extends XmlSplitter
{

    /** The format's name. */
    public static final String FORMAT = "ccda";

    /**
     * Creates the splitter; the service loader calls this.
     */
    public CcdaSplitter()
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String format()
    {
        return FORMAT;
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code ClinicalDocument}
     */
    @Override
    protected String defaultBoundary()
    {
        return "ClinicalDocument";
    }

    /**
     * {@inheritDoc}
     *
     * @return {@code urn:hl7-org:v3}
     */
    @Override
    protected String defaultNamespace()
    {
        return "urn:hl7-org:v3";
    }
}
