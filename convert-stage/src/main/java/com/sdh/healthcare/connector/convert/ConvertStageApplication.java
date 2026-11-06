/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The convert stage's Spring Boot entry point (STAGES.md §9).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootApplication
public class ConvertStageApplication
{

    /** Created by Spring. */
    public ConvertStageApplication()
    {
    }

    /**
     * Runs the stage.
     *
     * @param args the command line
     */
    public static void main(String[] args)
    {
        SpringApplication.run(ConvertStageApplication.class, args);
    }
}
