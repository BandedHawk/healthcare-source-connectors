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

    public static final String FORMAT = "ccda";

    @Override
    public String format()
    {
        return FORMAT;
    }

    @Override
    protected String defaultBoundary()
    {
        return "ClinicalDocument";
    }

    @Override
    protected String defaultNamespace()
    {
        return "urn:hl7-org:v3";
    }
}
