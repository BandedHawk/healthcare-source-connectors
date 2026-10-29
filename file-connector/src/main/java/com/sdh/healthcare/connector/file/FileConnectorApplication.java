package com.sdh.healthcare.connector.file;

import com.sdh.healthcare.connector.file.admin.AdminCli;
import java.util.Arrays;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
