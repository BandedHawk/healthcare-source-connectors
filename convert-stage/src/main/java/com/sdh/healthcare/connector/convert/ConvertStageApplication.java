/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import com.sdh.healthcare.connector.convert.admin.ConvertAdmin;
import java.util.Arrays;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The convert stage's Spring Boot entry point (STAGES.md §9). Started
 * normally, it runs the stage; started as {@code admin <command> ...}, it
 * runs that operator command instead and exits.
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
     * Runs the stage, or with {@code admin <command> …} an operator command
     * instead (design §11.4).
     *
     * @param args the command line
     */
    public static void main(String[] args)
    {
        if (args.length > 0 && args[0].equals("admin"))
        {
            System.exit(ConvertAdmin.cli().run(
                    Arrays.asList(args).subList(1, args.length), System.out,
                    System.err, System.getenv()));
        }
        SpringApplication.run(ConvertStageApplication.class, args);
    }
}
