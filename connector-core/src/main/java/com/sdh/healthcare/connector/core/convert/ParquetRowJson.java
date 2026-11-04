/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.convert;

import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Writes a row of a Parquet file, as Iceberg's generic reader gives it, as
 * a JSON object: mapping {@value #MAPPING} (design §6.4). Each column is a
 * key, in the schema's order, and a null is {@code null}:
 * <ul>
 * <li>booleans and whole numbers as they are; {@code float} and
 * {@code double} as numbers, except NaN and the infinities, which are the
 * strings {@code "NaN"}, {@code "Infinity"} and {@code "-Infinity"};</li>
 * <li>decimals as strings, digit for digit ({@code "12.50"}), so no
 * precision is lost to a reader that parses numbers as doubles;</li>
 * <li>dates, times and timestamps as ISO 8601 strings: {@code 2026-11-04},
 * {@code 10:15:30.25}, {@code 2026-11-04T10:15:30} without a zone, and
 * {@code 2026-11-04T10:15:30Z} for a timestamp adjusted to UTC;</li>
 * <li>strings and UUIDs as strings; binary and fixed as base64;</li>
 * <li>structs as objects; lists as arrays; maps with string keys as
 * objects, and other maps as arrays of {@code {"key":…,"value":…}}.</li>
 * </ul>
 * Variants, geometries and geographies have no JSON form here, and are
 * refused when the file is opened ({@link #check}).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class ParquetRowJson
{

    /** The name and version of the JSON. */
    public static final String MAPPING = "parquet-row/1";

    /** The JSON a row is expected to be about, to size the buffer. */
    private static final int ESTIMATE = 512;

    /** Types with no JSON form here, by name. */
    private static final Set<String> UNSUPPORTED =
            Set.of("VARIANT", "GEOMETRY", "GEOGRAPHY");

    /** Not made. */
    private ParquetRowJson()
    {
    }

    /**
     * Checks that every column has a JSON form.
     *
     * @param schema the file's schema
     * @return true if it has; false if a column, at any depth, is a variant,
     *         geometry or geography
     */
    public static boolean check(Schema schema)
    {
        return supported(schema.asStruct());
    }

    /**
     * Writes a row as JSON.
     *
     * @param row the row
     * @return the JSON, as UTF-8
     * @throws ConversionException if a value has no JSON form
     */
    public static byte[] write(Record row) throws ConversionException
    {
        return JsonWriter.build(ESTIMATE, writer -> struct(writer, row));
    }

    /**
     * Tells whether a type, and every type in it, has a JSON form.
     *
     * @param type the type
     * @return true if it has
     */
    private static boolean supported(Type type)
    {
        if (UNSUPPORTED.contains(type.typeId().name()))
        {
            return false;
        }
        if (type.isNestedType())
        {
            for (Types.NestedField field : type.asNestedType().fields())
            {
                if (!supported(field.type()))
                {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Writes a struct as an object.
     *
     * @param writer the writer
     * @param row    the struct
     * @throws ConversionException if a value has no JSON form
     */
    private static void struct(JsonWriter writer, Record row)
            throws ConversionException
    {
        writer.beginObject();
        List<Types.NestedField> fields = row.struct().fields();
        for (int index = 0; index < fields.size(); index++)
        {
            writer.key(fields.get(index).name());
            value(writer, row.get(index));
        }
        writer.endObject();
    }

    /**
     * Writes one value.
     *
     * @param writer the writer
     * @param value  the value, as the generic reader gives it
     * @throws ConversionException if it has no JSON form
     */
    private static void value(JsonWriter writer, Object value)
            throws ConversionException
    {
        switch (value)
        {
            case null -> writer.literal("null");
            case Boolean bool -> writer.literal(bool.toString());
            case Integer number -> writer.literal(number.toString());
            case Long number -> writer.literal(number.toString());
            case Float number -> floating(writer, number.isNaN(),
                    number.isInfinite(), number > 0, number.toString());
            case Double number -> floating(writer, number.isNaN(),
                    number.isInfinite(), number > 0, number.toString());
            case BigDecimal decimal -> writer.value(decimal.toPlainString());
            case CharSequence text -> writer.value(text.toString());
            case UUID uuid -> writer.value(uuid.toString());
            case LocalDate date -> writer.value(
                    DateTimeFormatter.ISO_LOCAL_DATE.format(date));
            case LocalTime time -> writer.value(
                    DateTimeFormatter.ISO_LOCAL_TIME.format(time));
            case LocalDateTime time -> writer.value(
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(time));
            case OffsetDateTime time -> writer.value(
                    DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
                            time.withOffsetSameInstant(ZoneOffset.UTC)));
            case byte[] bytes -> writer.value(
                    Base64.getEncoder().encodeToString(bytes));
            case ByteBuffer buffer -> writer.value(Base64.getEncoder()
                    .encodeToString(bytes(buffer)));
            case Record record -> struct(writer, record);
            case List<?> list ->
            {
                writer.beginArray();
                for (Object element : list)
                {
                    value(writer, element);
                }
                writer.endArray();
            }
            case Map<?, ?> map -> map(writer, map);
            default -> throw new ConversionException(
                    ErrorCode.PARSE_PARQUET_UNSUPPORTED_TYPE, 0);
        }
    }

    /**
     * Writes a float or double.
     *
     * @param writer   the writer
     * @param nan      whether it is NaN
     * @param infinite whether it is infinite
     * @param positive whether it is above zero
     * @param text     its usual text
     */
    private static void floating(JsonWriter writer, boolean nan,
            boolean infinite, boolean positive, String text)
    {
        if (nan)
        {
            writer.value("NaN");
        }
        else if (infinite)
        {
            writer.value(positive ? "Infinity" : "-Infinity");
        }
        else
        {
            writer.literal(text);
        }
    }

    /**
     * Writes a map: an object if every key is a string, otherwise an array
     * of key and value objects.
     *
     * @param writer the writer
     * @param map    the map
     * @throws ConversionException if a value has no JSON form
     */
    private static void map(JsonWriter writer, Map<?, ?> map)
            throws ConversionException
    {
        boolean textKeys = true;
        for (Object key : map.keySet())
        {
            textKeys &= key instanceof CharSequence;
        }
        if (textKeys)
        {
            writer.beginObject();
            for (Map.Entry<?, ?> entry : map.entrySet())
            {
                writer.key(entry.getKey().toString());
                value(writer, entry.getValue());
            }
            writer.endObject();
            return;
        }
        writer.beginArray();
        for (Map.Entry<?, ?> entry : map.entrySet())
        {
            writer.beginObject().key("key");
            value(writer, entry.getKey());
            writer.key("value");
            value(writer, entry.getValue());
            writer.endObject();
        }
        writer.endArray();
    }

    /**
     * A buffer's remaining bytes, leaving the buffer as it was.
     *
     * @param buffer the buffer
     * @return its bytes
     */
    private static byte[] bytes(ByteBuffer buffer)
    {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }
}
