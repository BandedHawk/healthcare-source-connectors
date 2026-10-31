/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.file;

import com.sdh.healthcare.connector.file.admin.AdminCli;
import java.util.Arrays;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The file connector's Spring Boot entry point. Started normally, it runs the connector; started
 * as {@code admin <command> ...}, it runs that operator command instead and exits.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootApplication
public class FileConnectorApplication {
    /** {@code admin <command> …} runs an operator command instead of the connector (design §11.4). */
    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("admin")) {
            System.exit(AdminCli.run(Arrays.asList(args).subList(1, args.length), System.out, System.err,
                    System.getenv()));
        }
        SpringApplication.run(FileConnectorApplication.class, args);
    }
}
