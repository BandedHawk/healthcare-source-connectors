/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.reader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.RecordSplit;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.TestPostgres;
import com.sdh.healthcare.connector.core.checkpoint.CheckpointCodec;
import com.sdh.healthcare.connector.core.checkpoint.ItemCheckpoints;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.RecordIds;
import com.sdh.healthcare.connector.core.pipeline.IngestPipeline;
import com.sdh.healthcare.connector.core.pipeline.PipelineSettings;
import com.sdh.healthcare.connector.core.publish.OutboundMessage;
import com.sdh.healthcare.connector.core.split.SeekableByteSource;
import com.sdh.healthcare.connector.core.split.SplitConfig;
import com.sdh.healthcare.connector.core.split.SplitterRegistry;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import java.security.KeyPair;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.crypto.KeyGenerator;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Test;

/**
 * Tests the whole path end to end (design §15): the connector writes, sends
 * a message, and {@code connector-queue-reader} reads the record back.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class PipelineToReaderTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    PipelineToReaderTest()
    {
    }

    /**
     * A file of 25 C-CDA documents is split into a record each, by the
     * split stage's pipeline (STAGES.md §8). A reader downstream reads each
     * document back from its {@code record.split}, as it was cut, and the
     * record's origin leads to its place in the file. Reading converted
     * records is tested with the convert stage.
     *
     * @throws Exception if a key can't be made or the file processed
     */
    @Test
    void downstreamReadsEachCcdaDocumentBackFromTheMessages() throws Exception
    {
        String template = "<ClinicalDocument><id root=\"%d\"/>"
                + "<title>Patient %d</title></ClinicalDocument>";
        StringBuilder file = new StringBuilder("<?xml version=\"1.0\"?>\n"
                + "<batch xmlns=\"urn:hl7-org:v3\">\n");
        List<String> expected = new ArrayList<>();
        for (int index = 0; index < 25; index++)
        {
            String document = template.formatted(index, index);
            file.append("  ").append(document).append('\n');
            // The splitter repeats the file's XML declaration on line 1,
            // and gives the root tag the batch's namespace.
            expected.add("<?xml version=\"1.0\"?>\n"
                    + document.replace("<ClinicalDocument>",
                            "<ClinicalDocument xmlns=\"urn:hl7-org:v3\">"));
        }
        file.append("</batch>\n");

        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        Table table = IngestTables.splitRecords(catalog, "sdh_split");
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        AesKeyWrapProvider kms = new AesKeyWrapProvider(
                Map.of("records-kek/1", generator.generateKey()));
        KeyPair signer = QueueReaderTest.ec();
        KeyPair recipient = QueueReaderTest.ec();
        LeaseToken lease = new LeaseToken()
        {

            /** Holds epoch 1, the only one. */
            public long epoch()
            {
                return 1;
            }

            /** Never finds the lease lost. */
            public void checkHeld()
            {
            }
        };
        RecordIds ids = new RecordIds(
                IdKeys.loadOrCreateShared(table, kms, "records-kek/1"));
        ItemCheckpoints checkpoints = new ItemCheckpoints(
                TestPostgres.get().getPostgresDatabase(), "reader_test",
                new CheckpointCodec(ids), Duration.ofSeconds(10));
        checkpoints.createTableIfMissing();
        List<OutboundMessage> sent =
                Collections.synchronizedList(new ArrayList<>());

        PipelineSettings settings = PipelineSettings
                .defaults("file-connector", "ccda", "prod", "records-kek/1")
                .withCommit(10, 1L << 30, Duration.ofHours(1))
                .withSplit(SplitConfig.defaults("ccda"));
        MessageSealer sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                "connector-2026", recipient.getPublic(), "downstream-2026");
        try (IngestPipeline pipeline = new IngestPipeline(settings, table,
                checkpoints, kms, ids, SplitterRegistry.load(), sealer,
                message ->
                {
                    sent.add(message);
                    return CompletableFuture.completedFuture(null);
                }, Clock.systemUTC()))
        {
            pipeline.split(
                    SeekableByteSource.of(file.toString().getBytes(UTF_8)),
                    UUID.randomUUID(), lease);
        }

        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", recipient.getPrivate())
                .trustedSigner("connector-2026",
                        (ECPublicKey) signer.getPublic())
                .catalog(catalog)
                .expectedCatalogName("prod")
                .keyProvider(kms)
                .build())
        {
            List<String> read = new ArrayList<>();
            for (OutboundMessage message : sent)
            {
                ConnectorMessage decoded = reader.decode(message.body());
                assertThat(decoded).isInstanceOf(RecordSplit.class);
                RecordSplit split = (RecordSplit) decoded;
                assertThat(split.errorCode()).isNull();
                // The id's place in the file: document i is on line 3 + i,
                // its id after "  <ClinicalDocument>"; in the record it is
                // on line 2, after the root tag with its namespace.
                assertThat(split.origin().toFile(new SourcePosition(2, 42)))
                        .isEqualTo(new SourcePosition(3 + split.seq(), 21));
                read.add(new String(reader.readRecord(split, 1 << 20),
                        UTF_8));
            }
            assertThat(read).containsExactlyElementsOf(expected);
        }
    }
}
