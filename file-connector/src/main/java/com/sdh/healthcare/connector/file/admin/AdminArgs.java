package com.sdh.healthcare.connector.file.admin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Command-line options for one admin command: {@code --name value} and {@code --flag}. Arguments of the form
 * {@code --a.b=c} are Spring configuration overrides and are passed through to the configuration instead.
 */
final class AdminArgs {
    private final String command;
    private final Map<String, String> options;
    private final Set<String> flags;
    private final List<String> configArgs;

    private AdminArgs(String command, Map<String, String> options, Set<String> flags, List<String> configArgs) {
        this.command = command;
        this.options = options;
        this.flags = flags;
        this.configArgs = configArgs;
    }

    /**
     * @param args      everything after {@code admin}
     * @param flagNames options that take no value
     */
    static AdminArgs parse(List<String> args, Set<String> flagNames) {
        if (args.isEmpty() || args.getFirst().startsWith("-")) {
            throw new UsageException("Missing command");
        }
        Map<String, String> options = new LinkedHashMap<>();
        java.util.Set<String> flags = new java.util.LinkedHashSet<>();
        List<String> config = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            String a = args.get(i);
            if (!a.startsWith("--") || a.length() == 2) {
                throw new UsageException("Unexpected argument: " + a);
            }
            if (a.contains("=")) {
                config.add(a);
                continue;
            }
            String name = a.substring(2);
            if (flagNames.contains(name)) {
                flags.add(name);
            } else if (i + 1 < args.size() && !args.get(i + 1).startsWith("--")) {
                if (options.put(name, args.get(++i)) != null) {
                    throw new UsageException("--" + name + " given twice");
                }
            } else {
                throw new UsageException("--" + name + " needs a value");
            }
        }
        return new AdminArgs(args.getFirst(), options, Set.copyOf(flags), List.copyOf(config));
    }

    String command() {
        return command;
    }

    List<String> configArgs() {
        return configArgs;
    }

    Optional<String> option(String name) {
        return Optional.ofNullable(options.get(name));
    }

    String required(String name) {
        return option(name).orElseThrow(() -> new UsageException("--" + name + " is required"));
    }

    boolean flag(String name) {
        return flags.contains(name);
    }

    /** Rejects options the command doesn't know, so a typo can't silently change what is restored. */
    void allowOnly(Set<String> names) {
        for (String n : options.keySet()) {
            if (!names.contains(n)) {
                throw new UsageException("Unknown option --" + n + " for " + command);
            }
        }
        for (String n : flags) {
            if (!names.contains(n)) {
                throw new UsageException("Unknown option --" + n + " for " + command);
            }
        }
    }

    /** A usage mistake: exit status 2 and the usage text. */
    static final class UsageException extends RuntimeException {
        UsageException(String message) {
            super(message);
        }
    }
}
