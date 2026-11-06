/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.reader;

/**
 * No rows for the record: removed by retention, wrong catalog, or the
 * commit is not visible yet.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public class RecordNotFoundException extends RuntimeException
{

    /**
     * Creates the exception.
     */
    public RecordNotFoundException()
    {
        super("SplitRecord not found");
    }
}
