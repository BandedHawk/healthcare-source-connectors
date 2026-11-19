/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract.table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RangedInputFile}.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class RangedInputFileTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    RangedInputFileTest()
    {
    }

    /** A missing object, as the S3 client names it. */
    static final class NoSuchKeyException extends RuntimeException
    {

        /** For serialization. */
        private static final long serialVersionUID = 1L;

        /** Creates the exception. */
        NoSuchKeyException()
        {
        }

    }

    /**
     * A file in memory whose streams read ranges, as Iceberg's object
     * store streams do, and note each request; a plain read fails.
     */
    static final class RangedFile implements InputFile
    {

        /** The file's bytes. */
        final byte[] bytes;

        /** Each request, as {start, length}; a tail as {-1, length}. */
        final List<long[]> requests = new ArrayList<>();

        /** Whether the object is gone. */
        boolean gone;

        /**
         * Holds the bytes.
         *
         * @param bytes the file's bytes
         */
        RangedFile(byte[] bytes)
        {
            this.bytes = bytes;
        }

        @Override
        public long getLength()
        {
            return bytes.length;
        }

        @Override
        public SeekableInputStream newStream()
        {
            return new Ranges();
        }

        @Override
        public String location()
        {
            return "memory://ranged";
        }

        @Override
        public boolean exists()
        {
            return !gone;
        }

        /** Reads by range only. */
        final class Ranges extends SeekableInputStream
                implements RangeReadable
        {

            /** Creates the stream. */
            Ranges()
            {
            }

            @Override
            public long getPos()
            {
                throw new AssertionError("Read by position");
            }

            @Override
            public void seek(long position)
            {
                throw new AssertionError("Read by position");
            }

            @Override
            public int read()
            {
                throw new AssertionError("Read by position");
            }

            @Override
            public void readFully(long position, byte[] buffer, int offset,
                    int length)
            {
                if (gone)
                {
                    throw new NoSuchKeyException();
                }
                requests.add(new long[] {position, length});
                System.arraycopy(bytes, (int) position, buffer, offset,
                        length);
            }

            @Override
            public int readTail(byte[] buffer, int offset, int length)
            {
                if (gone)
                {
                    throw new NoSuchKeyException();
                }
                requests.add(new long[] {-1, length});
                int count = Math.min(length, bytes.length);
                System.arraycopy(bytes, bytes.length - count, buffer, offset,
                        count);
                return count;
            }
        }
    }

    /**
     * Reads in the tail come from one request, read once; a read before it
     * reads ahead, and the reads after it within that come from memory,
     * one spanning the tail's start included; a long read is a request of
     * its own.
     *
     * @throws IOException if the test fails to run
     */
    @Test
    void readsTheTailOnceAndTheRestAhead() throws IOException
    {
        byte[] bytes = new byte[RangedInputFile.TAIL
                + 3 * RangedInputFile.READ_AHEAD];
        new Random(1).nextBytes(bytes);
        RangedFile file = new RangedFile(bytes);
        long tailStart = bytes.length - RangedInputFile.TAIL;
        try (org.apache.parquet.io.SeekableInputStream in =
                new RangedInputFile(file, bytes.length).newStream())
        {
            byte[] last = new byte[8];
            in.seek(bytes.length - 8);
            in.readFully(last);
            assertThat(last).isEqualTo(Arrays.copyOfRange(bytes,
                    bytes.length - 8, bytes.length));
            in.seek(tailStart + 100);
            ByteBuffer footer = ByteBuffer.allocate(1000);
            in.readFully(footer);
            assertThat(footer.array()).isEqualTo(Arrays.copyOfRange(bytes,
                    (int) tailStart + 100, (int) tailStart + 1100));
            assertThat(file.requests).hasSize(1);
            assertThat(file.requests.get(0)).containsExactly(-1,
                    RangedInputFile.TAIL);

            long near = tailStart - RangedInputFile.READ_AHEAD + 10;
            in.seek(near);
            byte[] page = new byte[500];
            in.readFully(page);
            assertThat(page).isEqualTo(Arrays.copyOfRange(bytes,
                    (int) near, (int) near + 500));
            assertThat(in.getPos()).isEqualTo(near + 500);
            assertThat(file.requests.get(1)).containsExactly(near,
                    tailStart - near);
            in.seek(tailStart - 50);
            ByteBuffer direct = ByteBuffer.allocateDirect(100);
            in.readFully(direct);
            direct.flip();
            byte[] spanned = new byte[100];
            direct.get(spanned);
            assertThat(spanned).isEqualTo(Arrays.copyOfRange(bytes,
                    (int) tailStart - 50, (int) tailStart + 50));
            assertThat(file.requests).hasSize(2);

            in.seek(5);
            byte[] large = new byte[RangedInputFile.READ_AHEAD + 1];
            in.readFully(large);
            assertThat(large).isEqualTo(Arrays.copyOfRange(bytes, 5,
                    6 + RangedInputFile.READ_AHEAD));
            assertThat(file.requests.get(2)).containsExactly(5,
                    RangedInputFile.READ_AHEAD + 1);

            in.seek(bytes.length - 1);
            assertThat(in.read()).isEqualTo(bytes[bytes.length - 1] & 0xff);
            assertThat(in.read()).isEqualTo(-1);
            assertThatThrownBy(() -> in.readFully(new byte[2]))
                    .isInstanceOf(EOFException.class);
            assertThat(file.requests).hasSize(3);
        }
    }

    /**
     * A file smaller than the tail is read whole, in one request.
     *
     * @throws IOException if the test fails to run
     */
    @Test
    void readsASmallFileInOneRequest() throws IOException
    {
        byte[] bytes = new byte[300];
        new Random(2).nextBytes(bytes);
        RangedFile file = new RangedFile(bytes);
        try (org.apache.parquet.io.SeekableInputStream in =
                new RangedInputFile(file, bytes.length).newStream())
        {
            byte[] all = new byte[300];
            in.readFully(all);
            assertThat(all).isEqualTo(bytes);
            in.seek(0);
            assertThat(in.read(new byte[400], 0, 400)).isEqualTo(300);
        }
        assertThat(file.requests).hasSize(1);
    }

    /**
     * A missing object is Iceberg's {@link NotFoundException}, as from its
     * plain stream; another failure is as it came.
     *
     * @throws IOException if the test fails to run
     */
    @Test
    void aMissingObjectIsNotFound() throws IOException
    {
        RangedFile file = new RangedFile(new byte[RangedInputFile.TAIL * 2]);
        file.gone = true;
        try (org.apache.parquet.io.SeekableInputStream in =
                new RangedInputFile(file, file.bytes.length).newStream())
        {
            assertThatThrownBy(() -> in.readFully(new byte[8]))
                    .isInstanceOf(NotFoundException.class);
            in.seek(file.bytes.length - 8);
            assertThatThrownBy(() -> in.readFully(new byte[8]))
                    .isInstanceOf(NotFoundException.class);
        }
        IllegalStateException other = new IllegalStateException();
        assertThat(RangedInputFile.Stream.gone(other)).isSameAs(other);
    }
}
