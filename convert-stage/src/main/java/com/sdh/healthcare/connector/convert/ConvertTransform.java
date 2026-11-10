/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.convert;

import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordInStorage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.message.StageMessages;
import com.sdh.healthcare.connector.core.convert.ConversionException;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.convert.RecordConverter;
import com.sdh.healthcare.connector.core.convert.Representation;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.telemetry.Attr;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import com.sdh.healthcare.connector.core.telemetry.Fields;
import com.sdh.healthcare.connector.core.telemetry.Telemetry;
import com.sdh.healthcare.connector.record.RecordTransform;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The convert stage's own step (design §6.4, STAGES.md §17.3): converts
 * a split record to JSON with its format's converter. A piece the
 * splitter couldn't parse, a record of a format with no converter, one
 * larger than {@code sdh.convert.max-bytes} and one two workers stopped
 * part-way through are stored as split. Everything else about the stage
 * is the shared record stage's.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ConvertTransform implements RecordTransform
{

    /** The mapping named in the version of records stored as split. */
    public static final String NO_MAPPING = "none";

    /**
     * The claim on which a record is stored unconverted: two workers
     * stopped part-way through it before, perhaps because of it.
     */
    public static final int SKIP_ON_ATTEMPT = 3;

    /** The converters, by format. */
    private final ConverterRegistry converters;

    /** How records are converted. */
    private final Settings settings;

    /** A digest of the settings that change a record's output. */
    private final String settingsDigest;

    /** Each format's settings, the options applied, once checked. */
    private final Map<String, SplitConfig> configs =
            new ConcurrentHashMap<>();

    /**
     * How records are converted.
     *
     * @param options  format options (design §6.2), for every format
     * @param maxBytes the largest record converted
     * @param build    the running build ({@code StageBuild}), the start of
     *                 each record's version
     */
    public record Settings(Map<String, String> options, long maxBytes,
            String build)
    {

        /**
         * No options is none, not null.
         *
         * @throws IllegalArgumentException if the largest record is not
         *                                  positive
         */
        public Settings
        {
            options = options == null ? Map.of() : Map.copyOf(options);
            if (maxBytes < 1)
            {
                throw new IllegalArgumentException(
                        "sdh.convert.max-bytes must be positive");
            }
        }
    }

    /**
     * Creates the transform.
     *
     * @param converters the converters, by format
     * @param settings   how records are converted
     */
    public ConvertTransform(ConverterRegistry converters, Settings settings)
    {
        this.converters = converters;
        this.settings = settings;
        this.settingsDigest = settingsDigest(settings);
    }

    /** The convert stage's: {@code record.split} in, {@code record.stored} out. */
    @Override
    public StageMessages messages()
    {
        return StageMessages.CONVERT;
    }

    /**
     * {@inheritDoc}
     *
     * @return the build, the mapping of the record's converter ({@code none}
     *         if its format has none) and a digest of the format options
     *         and the largest record converted, such as
     *         {@code 0.1.0@2026-11-06T10:15:30Z/ccda/2/3f9a0c1d22b7}
     */
    @Override
    public String version(RecordInStorage input)
    {
        String mapping = converters.get(input.format())
                .map(RecordConverter::mapping).orElse(NO_MAPPING);
        return settings.build() + "/" + mapping + "/" + settingsDigest;
    }

    /**
     * {@inheritDoc}
     *
     * @return one for each converter's mapping, and one for records stored
     *         as split
     */
    @Override
    public SortedSet<String> versions()
    {
        SortedSet<String> versions = new TreeSet<>();
        for (String mapping : converters.mappings())
        {
            versions.add(settings.build() + "/" + mapping + "/"
                    + settingsDigest);
        }
        versions.add(settings.build() + "/" + NO_MAPPING + "/"
                + settingsDigest);
        return versions;
    }

    @Override
    public long maxBytes()
    {
        return settings.maxBytes();
    }

    /** The record as split: its own format, unconverted. */
    @Override
    public Representation asIs(RecordInStorage input)
    {
        return Representation.raw(input.format());
    }

    /**
     * {@inheritDoc}
     *
     * A record on its {@value #SKIP_ON_ATTEMPT}rd claim is stored as split,
     * {@code CONVERT_SKIPPED}, audited {@code convert.record_skipped}.
     */
    @Override
    public Plan plan(RecordInStorage input, int attempt)
    {
        if (input instanceof RecordSplit split && split.errorCode() != null)
        {
            return new Plan.AsIs(splitError(split.errorCode()),
                    split.errorAt());
        }
        Optional<RecordConverter> found = converters.get(input.format());
        if (found.isEmpty())
        {
            return new Plan.AsIs(null, null);
        }
        if (attempt >= SKIP_ON_ATTEMPT)
        {
            Telemetry.get().auditWarn("convert.record_skipped",
                    Fields.of(Attr.ITEM_ID, input.itemId())
                            .with(Attr.SEQ, input.seq())
                            .with(Attr.FORMAT, input.format())
                            .with(Attr.ERROR_CODE, ErrorCode.CONVERT_SKIPPED));
            return new Plan.AsIs(ErrorCode.CONVERT_SKIPPED, null);
        }
        if (input.plaintextLength() > settings.maxBytes())
        {
            return new Plan.AsIs(ErrorCode.CONVERT_TOO_LARGE, null);
        }
        return new Plan.Apply((long) Math.ceil(input.plaintextLength()
                * found.get().memoryFactor()));
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException if the record's format has no converter
     */
    @Override
    public Result apply(RecordInStorage input, byte[] bytes)
    {
        RecordConverter converter = converters.get(input.format())
                .orElseThrow(() -> new IllegalStateException(
                        "No converter for the record's format"));
        try
        {
            return new Result.Made(converter.convert(bytes,
                    config(input.format())),
                    Representation.json(converter.mapping()));
        }
        catch (ConversionException failure)
        {
            return new Result.Failed(failure.code(), failure.position());
        }
    }

    /** {@code CONVERT_FAILED}: a converter's bug. */
    @Override
    public ErrorCode unexpected()
    {
        return ErrorCode.CONVERT_FAILED;
    }

    /**
     * The splitter's error, as a code. One this version doesn't know (a
     * newer splitter's) becomes {@code PARSE_UNRECOGNISED}, so the record is
     * still stored, failed.
     *
     * @param code the code in the message
     * @return the code
     */
    static ErrorCode splitError(String code)
    {
        try
        {
            return ErrorCode.valueOf(code);
        }
        catch (IllegalArgumentException unknown)
        {
            return ErrorCode.PARSE_UNRECOGNISED;
        }
    }

    /**
     * A format's settings with the stage's options applied, made once and
     * checked by its converter the first time.
     *
     * @param format the format
     * @return the settings
     */
    private SplitConfig config(String format)
    {
        return configs.computeIfAbsent(format, name ->
        {
            SplitConfig config = SplitConfig.defaults(name);
            for (Map.Entry<String, String> option
                    : settings.options().entrySet())
            {
                config = config.withOption(option.getKey(),
                        option.getValue());
            }
            SplitConfig checked = config;
            converters.get(name).ifPresent(
                    converter -> converter.validate(checked));
            return config;
        });
    }

    /**
     * A short digest of the settings that change a record's output, for
     * its version: the format options, in order of name, and the largest
     * record converted. A digest, so no setting's value reaches telemetry.
     * The same as before the move to the shared record stage, so a release
     * built either way stamps the same versions (STAGES.md §14.4a).
     *
     * @param settings the settings
     * @return the first 12 hex digits of the SHA-256
     */
    static String settingsDigest(Settings settings)
    {
        StringBuilder text = new StringBuilder();
        new TreeMap<>(settings.options()).forEach((name, value) ->
                text.append(name).append('=').append(value).append('\0'));
        text.append("max-bytes=").append(settings.maxBytes());
        try
        {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            // As the processor's output digest with no error or mapping.
            sha.update("-".getBytes(StandardCharsets.UTF_8));
            sha.update((byte) 0);
            sha.update("-".getBytes(StandardCharsets.UTF_8));
            sha.update((byte) 0);
            sha.update(text.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(sha.digest()).substring(0, 12);
        }
        catch (NoSuchAlgorithmException missing)
        {
            throw new IllegalStateException("SHA-256 unavailable", missing);
        }
    }
}
