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
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
import com.sdh.healthcare.connector.contract.position.SourcePosition;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
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
 * a message, and {@code connector-queue-reader} reads the section back.
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
     * A file of 25 C-CDA documents is split into a section each. A reader
     * downstream reads each document back from its message, as JSON with the
     * element positions, and the section's origin leads to its place in the
     * file.
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
            // Each document arrives as JSON in the C-CDA mapping (design
            // §6.4); ClinicalDocument has one id and title. Positions are in
            // the section: line 1 is the XML declaration the splitter adds,
            // and the root tag gains the batch's namespace, which moves the
            // id to column 42.
            expected.add(("{\"ClinicalDocument\":{\"#line\":2,\"#col\":1,"
                    + "\"id\":{\"#line\":2,\"#col\":42,\"root\":\"%d\"},"
                    + "\"title\":\"Patient %d\"}}").formatted(index, index));
        }
        file.append("</batch>\n");

        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION,
                "memory://wh-" + UUID.randomUUID()));
        Table table = IngestTables.sections(catalog, "sdh_ingest");
        KeyGenerator generator = KeyGenerator.getInstance("AES");
        generator.init(256);
        AesKeyWrapProvider kms = new AesKeyWrapProvider(
                Map.of("sections-kek/1", generator.generateKey()));
        KeyPair signer = QueueReaderTest.ec();
        KeyPair recipient = QueueReaderTest.ec();
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
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
        });
        committer.establish(table);
        SectionIds ids = new SectionIds(
                IdKeys.loadOrCreate(table, committer, kms, "sections-kek/1"));
        List<OutboundMessage> sent =
                Collections.synchronizedList(new ArrayList<>());

        PipelineSettings settings = PipelineSettings
                .defaults("file-connector", "ccda", "prod", "sections-kek/1")
                .withCommit(10, 1L << 30, Duration.ofHours(1))
                .withSplit(SplitConfig.defaults("ccda"));
        MessageSealer sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                "connector-2026", recipient.getPublic(), "downstream-2026");
        try (IngestPipeline pipeline = new IngestPipeline(settings, table,
                committer, kms, ids, SplitterRegistry.load(), sealer,
                message ->
                {
                    sent.add(message);
                    return CompletableFuture.completedFuture(null);
                }, Clock.systemUTC()))
        {
            pipeline.split(
                    SeekableByteSource.of(file.toString().getBytes(UTF_8)));
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
            ConnectorMessage first = reader.decode(sent.getFirst().body());
            assertThat(first).isInstanceOf(FileStarted.class);
            for (OutboundMessage message : sent.subList(1, sent.size()))
            {
                SectionStored stored =
                        (SectionStored) reader.decode(message.body());
                assertThat(stored.contentType()).isEqualTo("application/json");
                assertThat(stored.mapping()).isEqualTo("ccda/2");
                // The id's place in the file: document i is on line 3 + i,
                // its id after "  <ClinicalDocument>".
                assertThat(stored.origin().toFile(new SourcePosition(2, 42)))
                        .isEqualTo(new SourcePosition(3 + stored.seq(), 21));
                read.add(new String(reader.readSection(stored, 1 << 20),
                        UTF_8));
            }
            assertThat(read).containsExactlyElementsOf(expected);
        }
    }
}
