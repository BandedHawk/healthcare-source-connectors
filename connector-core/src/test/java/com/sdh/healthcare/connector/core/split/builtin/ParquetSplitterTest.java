/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.core.split.builtin;

import static com.sdh.healthcare.connector.core.split.SplitTesting.config;
import static com.sdh.healthcare.connector.core.split.SplitTesting.sections;
import static com.sdh.healthcare.connector.core.split.SplitTesting.texts;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.convert.ConverterRegistry;
import com.sdh.healthcare.connector.core.convert.ParquetRowJson;
import com.sdh.healthcare.connector.core.split.ResumePoint;
import com.sdh.healthcare.connector.core.split.Section;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitException;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.telemetry.ErrorCode;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ParquetSplitter} and {@link ParquetRowJson}, on files
 * written by Iceberg's and Parquet's own writers.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ParquetSplitterTest
{

    /** The splitter under test. */
    final ParquetSplitter splitter = new ParquetSplitter();

    /** A schema of a few columns. */
    static final Schema SIMPLE = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    /**
     * Writes rows with Iceberg's writer.
     *
     * @param schema   their schema
     * @param rows     the rows
     * @param oneGroup whether to keep them in one row group, rather than a
     *                 group for every few rows
     * @return the file
     * @throws IOException if it can't be written
     */
    static byte[] write(Schema schema, List<Record> rows, boolean oneGroup)
            throws IOException
    {
        InMemoryOutputFile out = new InMemoryOutputFile();
        Parquet.WriteBuilder builder = Parquet.write(out).schema(schema)
                .createWriterFunc(GenericParquetWriter::create);
        if (!oneGroup)
        {
            builder.set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "64")
                    .set(TableProperties
                            .PARQUET_ROW_GROUP_CHECK_MIN_RECORD_COUNT, "1")
                    .set(TableProperties
                            .PARQUET_ROW_GROUP_CHECK_MAX_RECORD_COUNT, "1");
        }
        try (FileAppender<Record> appender = builder.build())
        {
            appender.addAll(rows);
        }
        return out.toByteArray();
    }

    /**
     * Rows of the simple schema.
     *
     * @param count how many
     * @return the rows, with IDs from 1
     */
    static List<Record> simpleRows(int count)
    {
        List<Record> rows = new ArrayList<>();
        for (int i = 1; i <= count; i++)
        {
            Record row = GenericRecord.create(SIMPLE);
            row.setField("id", (long) i);
            row.setField("name", i % 3 == 0 ? null : "n" + i);
            rows.add(row);
        }
        return rows;
    }

    /**
     * Each row is one JSON section, across row groups, with its row number
     * as its origin; resuming after any section gives exactly the rest.
     *
     * @throws Exception if the test fails
     */
    @Test
    void splitsRowByRowAndResumesAfterAnyRow() throws Exception
    {
        byte[] file = write(SIMPLE, simpleRows(25), false);
        List<Section> all = sections(splitter, file, config("parquet"),
                ResumePoint.START);
        List<String> texts = texts(file, all);
        assertThat(texts).hasSize(25);
        assertThat(texts.get(0)).isEqualTo("{\"id\":1,\"name\":\"n1\"}");
        assertThat(texts.get(2)).isEqualTo("{\"id\":3,\"name\":null}");
        assertThat(all.stream().map(Section::sourceOffset).distinct())
                .hasSizeGreaterThan(3);
        assertThat(all.get(24).origin().inFile())
                .isEqualTo(new SourcePosition(25, 1));
        assertThat(all.get(24).resume().offset()).isEqualTo(file.length);
        for (int i = 0; i < all.size(); i++)
        {
            ResumePoint resume = ResumePoint.parse(
                    all.get(i).resume().encode());
            List<Section> rest = sections(splitter, file, config("parquet"),
                    resume);
            assertThat(texts(file, rest)).as("after row %d", i + 1)
                    .isEqualTo(texts.subList(i + 1, texts.size()));
            assertThat(rest).extracting(section -> section.origin().inFile())
                    .isEqualTo(all.subList(i + 1, all.size()).stream()
                            .map(section -> section.origin().inFile())
                            .toList());
        }
    }

    /**
     * Rows within one row group share an offset, and resume within it.
     *
     * @throws Exception if the test fails
     */
    @Test
    void resumesWithinARowGroup() throws Exception
    {
        byte[] file = write(SIMPLE, simpleRows(5), true);
        List<Section> all = sections(splitter, file, config("parquet"),
                ResumePoint.START);
        assertThat(all).extracting(Section::sourceOffset).containsOnly(4L);
        assertThat(all.get(1).resume()).isEqualTo(ResumePoint.at(4, 2));
        assertThat(texts(file, sections(splitter, file, config("parquet"),
                ResumePoint.at(4, 3)))).containsExactly(
                        "{\"id\":4,\"name\":\"n4\"}",
                        "{\"id\":5,\"name\":\"n5\"}");
    }

    /**
     * Every type with a JSON form is written as the mapping says.
     *
     * @throws Exception if the test fails
     */
    @Test
    void writesEachTypeAsTheMappingSays() throws Exception
    {
        Types.StructType address = Types.StructType.of(
                Types.NestedField.optional(20, "city", Types.StringType.get()));
        Schema schema = new Schema(
                Types.NestedField.optional(1, "b", Types.BooleanType.get()),
                Types.NestedField.optional(2, "i", Types.IntegerType.get()),
                Types.NestedField.optional(3, "f", Types.FloatType.get()),
                Types.NestedField.optional(4, "d", Types.DoubleType.get()),
                Types.NestedField.optional(5, "dec",
                        Types.DecimalType.of(9, 2)),
                Types.NestedField.optional(6, "date", Types.DateType.get()),
                Types.NestedField.optional(7, "time", Types.TimeType.get()),
                Types.NestedField.optional(8, "ts",
                        Types.TimestampType.withoutZone()),
                Types.NestedField.optional(9, "tstz",
                        Types.TimestampType.withZone()),
                Types.NestedField.optional(10, "uuid", Types.UUIDType.get()),
                Types.NestedField.optional(11, "bin", Types.BinaryType.get()),
                Types.NestedField.optional(12, "fix",
                        Types.FixedType.ofLength(2)),
                Types.NestedField.optional(13, "addr", address),
                Types.NestedField.optional(14, "tags", Types.ListType
                        .ofOptional(21, Types.StringType.get())),
                Types.NestedField.optional(15, "attrs", Types.MapType
                        .ofOptional(22, 23, Types.StringType.get(),
                                Types.IntegerType.get())),
                Types.NestedField.optional(16, "codes", Types.MapType
                        .ofOptional(24, 25, Types.IntegerType.get(),
                                Types.StringType.get())),
                Types.NestedField.optional(17, "s", Types.StringType.get()));
        Record row = GenericRecord.create(schema);
        row.setField("b", true);
        row.setField("i", 7);
        row.setField("f", Float.NaN);
        row.setField("d", 1.5);
        row.setField("dec", new BigDecimal("12.50"));
        row.setField("date", LocalDate.of(2026, 11, 4));
        row.setField("time", LocalTime.of(10, 15, 30, 250_000_000));
        row.setField("ts", LocalDateTime.of(2026, 11, 4, 10, 15, 30));
        row.setField("tstz", OffsetDateTime.of(2026, 11, 4, 12, 15, 30, 0,
                ZoneOffset.ofHours(2)));
        row.setField("uuid", UUID.fromString(
                "00000000-0000-0000-0000-000000000001"));
        row.setField("bin", ByteBuffer.wrap(new byte[] {1, 2, 3}));
        row.setField("fix", new byte[] {(byte) 0xff, 0});
        Record city = GenericRecord.create(address);
        city.setField("city", "Oslo");
        row.setField("addr", city);
        row.setField("tags", List.of("a", "b"));
        Map<String, Integer> attrs = new LinkedHashMap<>();
        attrs.put("x", 1);
        row.setField("attrs", attrs);
        row.setField("codes", Map.of(5, "five"));
        row.setField("s", "quote \" and é");
        byte[] file = write(schema, List.of(row), true);
        assertThat(texts(file, sections(splitter, file, config("parquet"),
                ResumePoint.START))).containsExactly("{\"b\":true,\"i\":7,"
                        + "\"f\":\"NaN\",\"d\":1.5,\"dec\":\"12.50\","
                        + "\"date\":\"2026-11-04\",\"time\":\"10:15:30.25\","
                        + "\"ts\":\"2026-11-04T10:15:30\","
                        + "\"tstz\":\"2026-11-04T10:15:30Z\","
                        + "\"uuid\":\"00000000-0000-0000-0000-000000000001\","
                        + "\"bin\":\"AQID\",\"fix\":\"/wA=\","
                        + "\"addr\":{\"city\":\"Oslo\"},\"tags\":[\"a\",\"b\"],"
                        + "\"attrs\":{\"x\":1},"
                        + "\"codes\":[{\"key\":5,\"value\":\"five\"}],"
                        + "\"s\":\"quote \\\" and é\"}");
    }

    /**
     * A file written without field IDs, by Parquet's own writer, is read
     * by its column names.
     *
     * @param dir where to write it
     * @throws Exception if the test fails
     */
    @Test
    void readsAFileWithoutFieldIds(@TempDir Path dir) throws Exception
    {
        MessageType type = MessageTypeParser.parseMessageType(
                "message row { required int64 id; optional binary name"
                        + " (STRING); }");
        Path path = dir.resolve("plain.parquet");
        try (ParquetWriter<Group> writer =
                ExampleParquetWriter.builder(new LocalOutputFile(path))
                        .withType(type).build())
        {
            SimpleGroupFactory groups = new SimpleGroupFactory(type);
            writer.write(groups.newGroup().append("id", 1L)
                    .append("name", "plain"));
        }
        byte[] file = Files.readAllBytes(path);
        assertThat(texts(file, sections(splitter, file, config("parquet"),
                ResumePoint.START)))
                .containsExactly("{\"id\":1,\"name\":\"plain\"}");
    }

    /**
     * A file that isn't Parquet, and a resume point that isn't a row, are
     * refused; a variant column has no JSON form.
     *
     * @throws Exception if the test fails
     */
    @Test
    void refusesWhatItCantRead() throws Exception
    {
        byte[] notParquet = "id,name\n1,x\n".getBytes(UTF_8);
        assertThatThrownBy(() -> sections(splitter, notParquet,
                config("parquet"), ResumePoint.START))
                .isInstanceOfSatisfying(SplitException.class,
                        failure -> assertThat(failure.code()).isEqualTo(
                                ErrorCode.PARSE_PARQUET_MALFORMED));
        byte[] file = write(SIMPLE, simpleRows(3), true);
        assertThatThrownBy(() -> sections(splitter, file, config("parquet"),
                ResumePoint.at(5)))
                .isInstanceOfSatisfying(SplitException.class,
                        failure -> assertThat(failure.code())
                                .isEqualTo(ErrorCode.RESUME_INVALID));
        assertThatThrownBy(() -> sections(splitter, file, config("parquet"),
                ResumePoint.at(4, 3)))
                .isInstanceOf(SplitException.class);
        assertThat(ParquetRowJson.check(new Schema(Types.NestedField
                .optional(1, "v", Types.VariantType.get())))).isFalse();
        assertThat(ParquetRowJson.check(SIMPLE)).isTrue();
    }

    /**
     * A failure to read the item is passed on as itself, to be tried
     * again, not taken as a broken file.
     *
     * @throws Exception if the test fails
     */
    @Test
    void passesOnAFailureToReadTheItem() throws Exception
    {
        byte[] file = write(SIMPLE, simpleRows(3), true);
        SeekableByteSource failing = new SeekableByteSource()
        {
            @Override
            public long size()
            {
                return file.length;
            }

            @Override
            public int read(long position, ByteBuffer dst) throws IOException
            {
                throw new IOException("storage down");
            }

            @Override
            public void close()
            {
            }
        };
        assertThatThrownBy(() -> SplitterRegistry.of(splitter)
                .open("parquet", failing, ResumePoint.START, 0,
                        config("parquet")))
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(SplitException.class)
                .hasMessage("storage down");
    }

    /**
     * The converter for the format passes the JSON on, labelled.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void labelsTheSectionsAsParquetRows() throws Exception
    {
        var converter = ConverterRegistry.load().get("parquet").orElseThrow();
        assertThat(converter.mapping()).isEqualTo("parquet-row/1");
        byte[] json = "{\"id\":1}".getBytes(UTF_8);
        byte[] out = converter.convert(json, config("parquet"));
        assertThat(out).isEqualTo(json).isNotSameAs(json);
    }

    /** A point is past another by offset, then by context. */
    @Test
    void ordersResumePoints()
    {
        assertThat(ResumePoint.at(5).isAfter(ResumePoint.at(4, 9))).isTrue();
        assertThat(ResumePoint.at(4, 2).isAfter(ResumePoint.at(4, 1)))
                .isTrue();
        assertThat(ResumePoint.at(4, 1).isAfter(ResumePoint.at(4))).isTrue();
        assertThat(ResumePoint.at(4).isAfter(ResumePoint.at(4))).isFalse();
        assertThat(ResumePoint.at(4, 1).isAfter(ResumePoint.at(4, 2)))
                .isFalse();
    }
}
