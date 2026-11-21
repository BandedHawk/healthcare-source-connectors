/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Command-line options for one admin command: {@code --name value} and
 * {@code --flag}. Arguments of the form {@code --a.b=c} are Spring
 * configuration overrides and are passed through to the configuration
 * instead.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class AdminArgs
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
     * What the command reads in place of standard input, when it is run
     * through the control API; null to read standard input.
     */
    private final InputStream input;

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
        this(command, options, flags, configArgs, null);
    }

    /**
     * Holds parsed arguments, with what is read in place of standard
     * input.
     *
     * @param command    the command name
     * @param options    the options given, by name
     * @param flags      the flags given
     * @param configArgs the configuration overrides
     * @param input      what is read in place of standard input; null for
     *                   standard input
     */
    private AdminArgs(String command, Map<String, String> options,
            Set<String> flags, List<String> configArgs, InputStream input)
    {
        this.command = command;
        this.options = options;
        this.flags = flags;
        this.configArgs = configArgs;
        this.input = input;
    }

    /**
     * The same arguments, with what the command reads in place of its
     * standard input: the control API's request body gives a reason or a
     * corrected record this way (CONTROL-API-DESIGN.md §5).
     *
     * @param given what is read
     * @return the arguments
     */
    public AdminArgs withInput(InputStream given)
    {
        return new AdminArgs(command, options, flags, configArgs, given);
    }

    /**
     * What the command reads its reason, record or key from: what was
     * given in its place, or else standard input as the context gives it.
     *
     * @param standard the context's standard input, asked for only if
     *                 nothing was given in its place
     * @return the stream
     */
    public InputStream input(Supplier<InputStream> standard)
    {
        return input != null ? input : standard.get();
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
    public static AdminArgs parse(List<String> args, Set<String> flagNames)
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
    public String command()
    {
        return command;
    }

    /**
     * The configuration overrides.
     *
     * @return the arguments of the form {@code --a.b=c}
     */
    public List<String> configArgs()
    {
        return configArgs;
    }

    /**
     * An option's value.
     *
     * @param name the option name, without {@code --}
     * @return the value, if given
     */
    public Optional<String> option(String name)
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
    public String required(String name)
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
    public boolean flag(String name)
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
    public void allowOnly(Set<String> names)
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
    public static final class UsageException extends RuntimeException
    {

        /**
         * Creates the exception.
         *
         * @param message what was wrong
         */
        public UsageException(String message)
        {
            super(message);
        }
    }
}
