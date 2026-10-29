package com.sdh.healthcare.connector.reader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.AesKeyWrapProvider;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.FileStarted;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.SectionStored;
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

/** Design §15: end to end, connector write → message → connector-queue-reader. */
class PipelineToReaderTest {

    @Test
    void downstreamReadsEachCcdaDocumentBackFromTheMessages() throws Exception {
        String doc = "<ClinicalDocument><id root=\"%d\"/><title>Patient %d</title></ClinicalDocument>";
        StringBuilder file = new StringBuilder("<?xml version=\"1.0\"?>\n<batch xmlns=\"urn:hl7-org:v3\">\n");
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            String d = doc.formatted(i, i);
            file.append("  ").append(d).append('\n');
            // Each document arrives as JSON in the C-CDA mapping (design §6.4); ClinicalDocument has one id and title.
            expected.add("{\"ClinicalDocument\":{\"id\":{\"root\":\"%d\"},\"title\":\"Patient %d\"}}".formatted(i, i));
        }
        file.append("</batch>\n");

        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("prod", Map.of(CatalogProperties.WAREHOUSE_LOCATION, "memory://wh-" + UUID.randomUUID()));
        Table table = IngestTables.sections(catalog, "sdh_ingest");
        KeyGenerator kg = KeyGenerator.getInstance("AES");
        kg.init(256);
        AesKeyWrapProvider kms = new AesKeyWrapProvider(Map.of("sections-kek/1", kg.generateKey()));
        KeyPair signer = QueueReaderTest.ec();
        KeyPair recipient = QueueReaderTest.ec();
        FencedCommitter committer = new FencedCommitter(new LeaseToken() {
            public long epoch() { return 1; }
            public void checkHeld() {}
        });
        committer.establish(table);
        SectionIds ids = new SectionIds(IdKeys.loadOrCreate(table, committer, kms, "sections-kek/1"));
        List<OutboundMessage> sent = Collections.synchronizedList(new ArrayList<>());

        PipelineSettings settings = PipelineSettings.defaults("file-connector", "ccda", "prod", "sections-kek/1")
                .withCommit(10, 1L << 30, java.time.Duration.ofHours(1))
                .withSplit(SplitConfig.defaults("ccda"));
        try (IngestPipeline pipeline = new IngestPipeline(settings, table, committer, kms, ids, SplitterRegistry.load(),
                new MessageSealer(new ECDSASigner((ECPrivateKey) signer.getPrivate()), "connector-2026",
                        recipient.getPublic(), "downstream-2026"),
                m -> {
                    sent.add(m);
                    return CompletableFuture.completedFuture(null);
                }, Clock.systemUTC())) {
            pipeline.split(SeekableByteSource.of(file.toString().getBytes(UTF_8)));
        }

        try (QueueReader reader = QueueReader.builder()
                .recipientKey("downstream-2026", recipient.getPrivate())
                .trustedSigner("connector-2026", (ECPublicKey) signer.getPublic())
                .catalog(catalog)
                .expectedCatalogName("prod")
                .keyProvider(kms)
                .build()) {
            List<String> read = new ArrayList<>();
            ConnectorMessage first = reader.decode(sent.getFirst().body());
            assertThat(first).isInstanceOf(FileStarted.class);
            for (OutboundMessage m : sent.subList(1, sent.size())) {
                SectionStored s = (SectionStored) reader.decode(m.body());
                assertThat(s.contentType()).isEqualTo("application/json");
                assertThat(s.mapping()).isEqualTo("ccda/1");
                read.add(new String(reader.readSection(s, 1 << 20), UTF_8));
            }
            assertThat(read).containsExactlyElementsOf(expected);
        }
    }
}
