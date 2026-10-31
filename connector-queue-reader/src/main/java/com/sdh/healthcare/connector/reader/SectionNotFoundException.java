/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.reader;

/**
 * No rows for the section: removed by retention, wrong catalog, or the commit is not visible yet.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class SectionNotFoundException extends RuntimeException {
    public SectionNotFoundException() {
        super("Section not found");
    }
}
