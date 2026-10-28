package com.sdh.healthcare.connector.core.split.builtin;

/** {@link XmlSplitter} with C-CDA defaults: element {@code ClinicalDocument} in namespace {@code urn:hl7-org:v3}. */
public final class CcdaSplitter extends XmlSplitter {
    public static final String FORMAT = "ccda";

    @Override
    public String format() {
        return FORMAT;
    }

    @Override
    protected String defaultBoundary() {
        return "ClinicalDocument";
    }

    @Override
    protected String defaultNamespace() {
        return "urn:hl7-org:v3";
    }
}
