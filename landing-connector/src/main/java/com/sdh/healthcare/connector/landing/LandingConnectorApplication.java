/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing;

import com.sdh.healthcare.connector.landing.admin.LandingAdmin;
import java.util.Arrays;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The landing node's Spring Boot entry point (design §5.3): one node per
 * intake, landing what arrives as it is, and telling the splitter with
 * {@code item.landed}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@SpringBootApplication
public class LandingConnectorApplication
{

    /**
     * Starts the node, or with {@code admin <command> …} an operator
     * command instead (design §11.4).
     *
     * @param arguments Spring Boot's command-line arguments
     */
    public static void main(String[] arguments)
    {
        if (arguments.length > 0 && arguments[0].equals("admin"))
        {
            System.exit(LandingAdmin.cli().run(
                    Arrays.asList(arguments).subList(1, arguments.length),
                    System.out, System.err, System.getenv()));
        }
        SpringApplication.run(LandingConnectorApplication.class, arguments);
    }
}
