/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.runtime.compliance;

import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sun.management.HotSpotDiagnosticMXBean;
import com.sun.management.VMOption;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.context.properties.source.ConfigurationProperty;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.context.properties.source.IterableConfigurationPropertySource;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

/**
 * Applies the compliance profile when a node starts (design §7.6), before
 * any other bean is made, so a refused setting stops the node before it
 * connects to anything. Imported by each application's configuration.
 *
 * <p>A refusal is logged as one {@code compliance.refused} audit event per
 * finding, naming the check and the setting, never a value, and startup
 * fails with the same list. Telemetry isn't exported yet at that point, so
 * these reach the console only. Once the node has started, the profile,
 * each waiver and its reason, or that the profile is off, are audited
 * through the exporter as well.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
public class ComplianceConfiguration
{

    /** Where the profile's summary is logged. */
    private static final Logger LOG =
            LoggerFactory.getLogger("sdh.compliance");

    /** The prefixes of the settings the profile reads. */
    private static final List<String> PREFIXES =
            List.of("sdh.", "camel.component.", "spring.rabbitmq.");

    /** Created by Spring. */
    public ComplianceConfiguration()
    {
    }

    /**
     * The guard that applies the profile, before other beans are made.
     *
     * @return the guard
     */
    @Bean
    static ComplianceGuard sdhComplianceGuard()
    {
        return new ComplianceGuard(ComplianceConfiguration::jvmOption);
    }

    /**
     * Audits what the profile found once the node has started, when
     * telemetry is exported.
     *
     * @return the audit
     */
    @Bean
    static SmartInitializingSingleton sdhComplianceAudit()
    {
        return () -> audit(ComplianceGuard.lastOutcome());
    }

    /**
     * The settings the profile reads, by canonical name, however they were
     * set (files, the environment, arguments), each with the value in
     * force.
     *
     * @param environment the node's environment
     * @return the settings, sorted by name
     */
    static Map<String, String> settings(ConfigurableEnvironment environment)
    {
        List<ConfigurationPropertySource> sources = new ArrayList<>();
        ConfigurationPropertySources.get(environment).forEach(sources::add);
        Map<String, String> settings = new TreeMap<>();
        for (ConfigurationPropertySource source : sources)
        {
            if (!(source instanceof IterableConfigurationPropertySource names))
            {
                continue;
            }
            names.stream().filter(name -> PREFIXES.stream()
                    .anyMatch(name.toString()::startsWith))
                    .filter(name -> !settings.containsKey(name.toString()))
                    .forEach(name -> value(environment, sources, name)
                            .ifPresent(v -> settings.put(name.toString(), v)));
        }
        // An environment variable's name gives dots where the setting has
        // dashes (SDH_CRYPTO_WRAP_PROVIDER, sdh.crypto.wrap.provider), so
        // the settings the rules read by name are looked up by name.
        for (String known : ComplianceRules.namedSettings())
        {
            value(environment, sources, ConfigurationPropertyName.of(known))
                    .ifPresent(v -> settings.put(known, v));
        }
        return settings;
    }

    /**
     * A setting's value in force: from the first source, in precedence
     * order, that has it, with placeholders resolved.
     *
     * @param environment the environment, to resolve placeholders
     * @param sources     its sources, in precedence order
     * @param name        the setting's canonical name
     * @return the value; empty if it is unset or isn't text
     */
    private static Optional<String> value(Environment environment,
            List<ConfigurationPropertySource> sources,
            ConfigurationPropertyName name)
    {
        for (ConfigurationPropertySource source : sources)
        {
            ConfigurationProperty property =
                    source.getConfigurationProperty(name);
            if (property != null && property.getValue() != null)
            {
                return Optional.of(environment.resolvePlaceholders(
                        property.getValue().toString()));
            }
        }
        return Optional.empty();
    }

    /**
     * A JVM option's value, from HotSpot.
     *
     * @param name the option, such as {@code HeapDumpOnOutOfMemoryError}
     * @return its value; empty if the JVM doesn't have it or won't say
     */
    static Optional<String> jvmOption(String name)
    {
        try
        {
            HotSpotDiagnosticMXBean hotspot = ManagementFactory
                    .getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            if (hotspot == null)
            {
                return Optional.empty();
            }
            VMOption option = hotspot.getVMOption(name);
            return Optional.ofNullable(option.getValue());
        }
        catch (IllegalArgumentException | SecurityException e)
        {
            return Optional.empty();
        }
    }

    /**
     * Audits what the profile found, once the node has started.
     *
     * @param outcome the outcome; null if the guard didn't run
     */
    static void audit(ComplianceRules.Outcome outcome)
    {
        if (outcome == null)
        {
            return;
        }
        Telemetry telemetry = Telemetry.get();
        Fields profile = Fields.of(Attr.PROFILE, outcome.profile().id());
        if (outcome.profile() == ComplianceProfile.NONE)
        {
            telemetry.auditWarn("compliance.off", profile);
            return;
        }
        outcome.waived().forEach((finding, reason) ->
                telemetry.auditWarn("compliance.waived", profile
                        .with(Attr.CHECK, finding.check().id())
                        .with(Attr.SETTING, finding.setting())
                        .with(Attr.WAIVER_REASON, reason)));
        telemetry.audit("compliance.checked", profile
                .with(Attr.COUNT, outcome.waived().size()));
    }

    /**
     * Applies the profile to the environment's settings before any other
     * bean is made, and stops startup if it refuses any.
     *
     * @author Jonas Barnett
     * @since 0.1.0
     */
    public static final class ComplianceGuard
            implements BeanFactoryPostProcessor, EnvironmentAware
    {

        /** The outcome of the last check, for the audit after startup. */
        private static volatile ComplianceRules.Outcome last;

        /** Reads JVM options. */
        private final Function<String, Optional<String>> jvmOption;

        /** The node's environment. */
        private ConfigurableEnvironment environment;

        /**
         * Creates the guard.
         *
         * @param jvmOption reads a JVM option's value
         */
        public ComplianceGuard(Function<String, Optional<String>> jvmOption)
        {
            this.jvmOption = jvmOption;
        }

        /**
         * The outcome of the last check in this process.
         *
         * @return the outcome; null if none has run
         */
        static ComplianceRules.Outcome lastOutcome()
        {
            return last;
        }

        /**
         * Keeps the environment to read the settings from.
         *
         * @param environment the node's environment
         */
        @Override
        public void setEnvironment(Environment environment)
        {
            this.environment = (ConfigurableEnvironment) environment;
        }

        /**
         * Applies the profile.
         *
         * @param beanFactory not used
         * @throws IllegalStateException if the profile refuses a setting,
         *                               naming each check and setting
         */
        @Override
        public void postProcessBeanFactory(
                ConfigurableListableBeanFactory beanFactory)
        {
            ComplianceRules.Outcome outcome = ComplianceRules.evaluate(
                    settings(environment), jvmOption);
            last = outcome;
            if (!outcome.passed())
            {
                Telemetry telemetry = Telemetry.get();
                for (ComplianceRules.Finding finding : outcome.refused())
                {
                    telemetry.auditError("compliance.refused",
                            Fields.of(Attr.PROFILE, outcome.profile().id())
                                    .with(Attr.CHECK, finding.check().id())
                                    .with(Attr.SETTING, finding.setting()));
                }
                throw new IllegalStateException(refusal(outcome));
            }
            if (outcome.profile() == ComplianceProfile.NONE)
            {
                LOG.warn("Compliance profile none: no checks. For"
                        + " deployments that hold no PHI only (design"
                        + " §7.6).");
            }
            else
            {
                LOG.info("Compliance profile {}: passed, {} waived{}",
                        outcome.profile().id(), outcome.waived().size(),
                        outcome.waived().isEmpty() ? ""
                                : " (" + outcome.waived().keySet().stream()
                                        .map(f -> f.check().id())
                                        .distinct()
                                        .collect(Collectors.joining(", "))
                                        + ")");
            }
        }

        /**
         * The startup error for a refusal.
         *
         * @param outcome the outcome
         * @return the message: each check and setting, never a value
         */
        static String refusal(ComplianceRules.Outcome outcome)
        {
            StringBuilder message = new StringBuilder(ComplianceRules.PROFILE
                    + "=" + outcome.profile().id()
                    + " refuses these settings (design §7.6). Fix each, or"
                    + " accept it by setting " + ComplianceRules.WAIVE
                    + "<check> to the reason:");
            for (ComplianceRules.Finding finding : outcome.refused())
            {
                message.append("\n  ").append(finding.describe());
            }
            return message.toString();
        }
    }
}
