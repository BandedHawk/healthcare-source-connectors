package com.sdh.healthcare.connector.core.storage;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.pipeline.RetryPolicy;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * When a batch's Parquet file fails to close, the error that caused it must be the one reported. Parquet refuses a
 * second close of a failed writer ("Cannot return unfinished footer"); the cleanup path used to close again and
 * throw that instead, which hid an out-of-memory error during the heap-sizing runs (design §13.1).
 */
class AbandonedBatchTest {
    static final String FAILURE = "disk full (test)";

    AesKeyWrapProvider keys;
    InMemoryCatalog catalog;

    @BeforeEach
    void setUp() throws Exception {
        catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://wh-" + UUID.randomUUID()));
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        keys = new AesKeyWrapProvider(Map.of("k", kg.generateKey()));
    }

    @Test
    void archiveReportsTheWriteFailureNotTheSecondClose() {
        Table table = failingClose(IngestTables.sourceFiles(catalog, "sdh_ingest"));
        FencedCommitter committer = new FencedCommitter(new LeaseToken() {
            public long epoch() { return 1; }
            public void checkHeld() {}
        });
        committer.establish(table);
        Archiver archiver = new Archiver(table, committer, keys, new SectionIds(new byte[32]),
                Archiver.Settings.defaults("file-connector", "k").withSizes(1024, 4096, 4096)
                        .withRetry(new RetryPolicy(1, Duration.ofMillis(1), Duration.ofMillis(1))),
                Clock.systemUTC());

        assertThatThrownBy(() -> archiver.archive(UUID.randomUUID(), SeekableByteSource.of(new byte[5000]),
                Optional.empty(), (r, at) -> {}))
                .hasRootCauseMessage(FAILURE);
    }

    @Test
    void sectionBatchCloseAfterFailedFinishDoesNotThrow() {
        Table table = failingClose(IngestTables.sections(catalog, "sdh_ingest"));
        SectionBatchWriter w = new SectionBatchWriter(table, keys, "k", new SectionIds(new byte[32]), Instant.now());
        w.add(new SectionBatchWriter.SectionInput(UUID.randomUUID(), 0, 0, "csv", "a,b".getBytes()));

        assertThatThrownBy(w::finish).hasRootCauseMessage(FAILURE);
        assertThatCode(w::close).doesNotThrowAnyException();
    }

    /**
     * The table, with every Parquet data file failing once its row data is written: after the 4-byte header, before
     * the footer, as an out-of-memory error part-way through a row group would. Metadata files are untouched.
     */
    static Table failingClose(Table table) {
        TableOperations ops = ((HasTableOperations) table).operations();
        FileIO io = new FileIO() {
            @Override
            public InputFile newInputFile(String path) {
                return ops.io().newInputFile(path);
            }

            @Override
            public OutputFile newOutputFile(String path) {
                OutputFile real = ops.io().newOutputFile(path);
                if (!path.endsWith(".parquet")) {
                    return real;
                }
                return new OutputFile() {
                    @Override
                    public PositionOutputStream create() {
                        return failing(real.create());
                    }

                    @Override
                    public PositionOutputStream createOrOverwrite() {
                        return failing(real.createOrOverwrite());
                    }

                    @Override
                    public String location() {
                        return real.location();
                    }

                    @Override
                    public InputFile toInputFile() {
                        return real.toInputFile();
                    }
                };
            }

            @Override
            public void deleteFile(String path) {
                ops.io().deleteFile(path);
            }
        };
        TableOperations failing = new TableOperations() {
            @Override
            public TableMetadata current() {
                return ops.current();
            }

            @Override
            public TableMetadata refresh() {
                return ops.refresh();
            }

            @Override
            public void commit(TableMetadata base, TableMetadata metadata) {
                ops.commit(base, metadata);
            }

            @Override
            public FileIO io() {
                return io;
            }

            @Override
            public String metadataFileLocation(String fileName) {
                return ops.metadataFileLocation(fileName);
            }

            @Override
            public LocationProvider locationProvider() {
                return ops.locationProvider();
            }
        };
        return new BaseTable(failing, table.name());
    }

    static PositionOutputStream failing(PositionOutputStream out) {
        return new PositionOutputStream() {
            @Override
            public long getPos() throws IOException {
                return out.getPos();
            }

            @Override
            public void write(int b) throws IOException {
                check(1);
                out.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                check(len);
                out.write(b, off, len);
            }

            private void check(int len) throws IOException {
                if (out.getPos() + len > 4) {
                    throw new IOException(FAILURE);
                }
            }

            @Override
            public void close() throws IOException {
                out.close();
            }
        };
    }
}
