package com.sdh.healthcare.connector.reader;

/** No rows for the section: removed by retention, wrong catalog, or the commit is not visible yet. */
public class SectionNotFoundException extends RuntimeException {
    public SectionNotFoundException() {
        super("Section not found");
    }
}
