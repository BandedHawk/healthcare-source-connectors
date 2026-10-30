package com.sdh.healthcare.connector.core.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.core.TestCatalogs;
import com.sdh.healthcare.connector.core.storage.IngestTables;
import java.util.UUID;
import org.apache.iceberg.BaseTransaction;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Stops an Iceberg or Parquet upgrade from going in unreviewed (design §9.3, §16). The fencing uses Iceberg methods
 * that aren't formally part of its API, and other code depends on Iceberg behaviour that isn't promised either. The
 * fencing tests run on every build, but they can't notice everything (a REST server's commit checks, ADLS listing),
 * so a version change fails here until the review below has been done and the reviewed versions updated.
 */
class IcebergUpgradeGuardTest {

    /** The versions last reviewed against the list in {@link #REVIEW}. */
    static final String REVIEWED_ICEBERG = "1.12.0";
    static final String REVIEWED_PARQUET = "1.17.1";

    static final String REVIEW = """
            Iceberg or Parquet changed version. Before updating the reviewed versions in IcebergUpgradeGuardTest:
             1. Fencing (design §9.3): read the release notes and the changes to BaseTransaction (startMetadata,
                currentMetadata, underlyingOps), TableOperations.commit and the REST catalog's commit requirements
                (UpdateRequirements). FencedCommitter relies on a strict compare-and-swap and on REST checking
                assert-ref-snapshot-id when main moves.
             2. FencedCommitterTest must pass unchanged, including plainIcebergTransactionIsNotFenced: if that
                control test starts failing, Iceberg's commit behaviour changed and §9.3 point 3 needs another look.
             3. Run the integration tests against real catalogs and storage: PolarisIT (a REST server), the S3, GCS
                and ADLS ITs, and AdlsAzureIT on a real account (ADLSFileIO's listing and orphan cleanup, §8.4).
             4. Set parquet.version in the parent POM to the Parquet version the new Iceberg declares
                (its iceberg-parquet POM), and check parquet-jackson's bundled Jackson (vuln-accepted.yaml, §14.5).
             5. Update the reviewed versions here and the versions in design §2.
            """;

    @Test
    void icebergIsTheReviewedVersion() {
        assertThat(IcebergBuild.version()).as(REVIEW).isEqualTo(REVIEWED_ICEBERG);
    }

    @Test
    void parquetIsTheReviewedVersion() throws Exception {
        // Read at run time: VERSION_NUMBER is a constant, which the compiler would copy into this class.
        Object parquet = org.apache.parquet.Version.class.getField("VERSION_NUMBER").get(null);
        assertThat(parquet).as(REVIEW).isEqualTo(REVIEWED_PARQUET);
    }

    /** FencedCommitter needs BaseTransaction to reach the start metadata; anything else would fail every commit. */
    @ParameterizedTest
    @EnumSource(TestCatalogs.Kind.class)
    void transactionsAreBaseTransactions(TestCatalogs.Kind kind) {
        String ns = "ns_" + UUID.randomUUID().toString().replace("-", "");
        Table table = IngestTables.sections(TestCatalogs.create(kind).newClient(), ns);
        assertThat(table.newTransaction()).as(REVIEW).isInstanceOf(BaseTransaction.class);
    }
}
