/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file.admin;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Command-line options for one admin command: {@code --name value} and
 * {@code --flag}. Arguments of the form {@code --a.b=c} are Spring
 * configuration overrides and are passed through to the configuration
 * instead.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
final class AdminArgs
{

    /** The command name. */
    private final String command;

    /** The options given, by name. */
    private final Map<String, String> options;

    /** The flags given. */
    private final Set<String> flags;

    /** The configuration overrides, as given. */
    private final List<String> configArgs;

    /**
     * Holds parsed arguments.
     *
     * @param command the command name
     * @param options the options given, by name
     * @param flags the flags given
     * @param configArgs the configuration overrides
     */
    private AdminArgs(String command, Map<String, String> options,
            Set<String> flags, List<String> configArgs)
    {
        this.command = command;
        this.options = options;
        this.flags = flags;
        this.configArgs = configArgs;
    }

    /**
     * Parses a command line.
     *
     * @param args everything after {@code admin}
     * @param flagNames options that take no value
     * @return the parsed arguments
     * @throws UsageException if the command is missing or an argument is
     *         malformed, repeated or lacks its value
     */
    static AdminArgs parse(List<String> args, Set<String> flagNames)
    {
        if (args.isEmpty() || args.getFirst().startsWith("-"))
        {
            throw new UsageException("Missing command");
        }
        Map<String, String> options = new LinkedHashMap<>();
        Set<String> flags = new LinkedHashSet<>();
        List<String> config = new ArrayList<>();
        for (int index = 1; index < args.size(); index++)
        {
            String argument = args.get(index);
            if (!argument.startsWith("--") || argument.length() == 2)
            {
                throw new UsageException("Unexpected argument: " + argument);
            }
            if (argument.contains("="))
            {
                config.add(argument);
                continue;
            }
            String name = argument.substring(2);
            if (flagNames.contains(name))
            {
                flags.add(name);
            }
            else if (index + 1 < args.size()
                    && !args.get(index + 1).startsWith("--"))
            {
                if (options.put(name, args.get(++index)) != null)
                {
                    throw new UsageException("--" + name + " given twice");
                }
            }
            else
            {
                throw new UsageException("--" + name + " needs a value");
            }
        }
        return new AdminArgs(args.getFirst(), options, Set.copyOf(flags),
                List.copyOf(config));
    }

    /**
     * The command name.
     *
     * @return the name
     */
    String command()
    {
        return command;
    }

    /**
     * The configuration overrides.
     *
     * @return the arguments of the form {@code --a.b=c}
     */
    List<String> configArgs()
    {
        return configArgs;
    }

    /**
     * An option's value.
     *
     * @param name the option name, without {@code --}
     * @return the value, if given
     */
    Optional<String> option(String name)
    {
        return Optional.ofNullable(options.get(name));
    }

    /**
     * A required option's value.
     *
     * @param name the option name, without {@code --}
     * @return the value
     * @throws UsageException if the option wasn't given
     */
    String required(String name)
    {
        return option(name).orElseThrow(
                () -> new UsageException("--" + name + " is required"));
    }

    /**
     * Whether a flag was given.
     *
     * @param name the flag name, without {@code --}
     * @return true if given
     */
    boolean flag(String name)
    {
        return flags.contains(name);
    }

    /**
     * Rejects options the command doesn't know, so a typo can't silently
     * change what is restored.
     *
     * @param names the options and flags the command accepts
     * @throws UsageException for the first unknown one
     */
    void allowOnly(Set<String> names)
    {
        for (String name : options.keySet())
        {
            if (!names.contains(name))
            {
                throw new UsageException(
                        "Unknown option --" + name + " for " + command);
            }
        }
        for (String name : flags)
        {
            if (!names.contains(name))
            {
                throw new UsageException(
                        "Unknown option --" + name + " for " + command);
            }
        }
    }

    /** A usage mistake: exit status 2 and the usage text. */
    static final class UsageException extends RuntimeException
    {

        /**
         * Creates the exception.
         *
         * @param message what was wrong
         */
        UsageException(String message)
        {
            super(message);
        }
    }
}
