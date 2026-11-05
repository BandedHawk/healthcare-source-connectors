/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.contract;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.jose.MessageOpener;
import com.sdh.healthcare.connector.contract.jose.MessageRejectedException;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.ItemLanded;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage.StorageRef;
import com.sdh.healthcare.connector.contract.message.PayloadCodec;
import com.sdh.healthcare.connector.contract.message.UnsupportedContractVersionException;
import java.security.KeyPair;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * How {@code item.landed} payloads of other contract versions are read
 * (design §10.2, docs/DEPLOYMENT.md): the landing node and the splitter
 * are released separately, so for a while one may be newer than the
 * other. A later minor version adds only optional fields, which an older
 * reader ignores; an earlier minor version lacks fields added since, which
 * read as unset; another major version is refused, with
 * {@code UNSUPPORTED_VERSION} as the reason.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class ContractVersionTest
{

    /** Creates the tests; JUnit runs each test on a new instance. */
    ContractVersionTest()
    {
    }

    /**
     * An {@code item.landed} message that replaces another, so that every
     * field is set.
     *
     * @return the message
     */
    static ItemLanded landed()
    {
        Instant landedAt = Instant.parse("2026-11-05T10:15:30Z");
        return new ItemLanded(UUID.randomUUID(), "claims", "file", 1234,
                "digest-abcdefgh", landedAt,
                new StorageRef("sdh", "sdh_landing", "landed_items",
                        LocalDate.of(2026, 11, 5), 77, "kek/1"),
                UUID.randomUUID());
    }

    /**
     * The message's payload, as this build writes it, but marked with
     * another version.
     *
     * @param message the message
     * @param version the contract version to mark it with
     * @return the payload, which the caller may change further
     */
    static Map<String, Object> payload(ConnectorMessage message,
            String version)
    {
        Map<String, Object> payload =
                new LinkedHashMap<>(PayloadCodec.toJson(message));
        payload.put("v", version);
        return payload;
    }

    /**
     * Only the same major version is readable, whatever the minor.
     */
    @Test
    void readsTheSameMajorVersionOnly()
    {
        int major = ContractVersion.MAJOR;
        assertThat(ContractVersion.isReadable(ContractVersion.CURRENT))
                .isTrue();
        assertThat(ContractVersion.isReadable(major + ".0")).isTrue();
        assertThat(ContractVersion.isReadable(major + ".99")).isTrue();
        assertThat(ContractVersion.isReadable(String.valueOf(major)))
                .isTrue();
        assertThat(ContractVersion.isReadable((major + 1) + ".0")).isFalse();
        assertThat(ContractVersion.isReadable((major - 1) + ".9")).isFalse();
        assertThat(ContractVersion.isReadable(null)).isFalse();
        assertThat(ContractVersion.isReadable("")).isFalse();
        assertThat(ContractVersion.isReadable("two.four")).isFalse();
    }

    /**
     * A later minor version, with a field this build doesn't know, reads
     * as the message it carries; the field is ignored.
     */
    @Test
    void laterMinorVersionIsReadAndUnknownFieldsIgnored()
    {
        ItemLanded sent = landed();
        Map<String, Object> payload = payload(sent,
                ContractVersion.MAJOR + "." + (ContractVersion.MINOR + 5));
        payload.put("priority", "high");
        payload.put("lineage", Map.of("hops", 3));

        assertThat(PayloadCodec.fromJson(payload)).isEqualTo(sent);
    }

    /**
     * An earlier minor version, from before {@code replaces} was added
     * (2.3), reads with no earlier item.
     */
    @Test
    void earlierMinorVersionReadsMissingFieldsAsUnset()
    {
        ItemLanded sent = landed();
        Map<String, Object> payload =
                payload(sent, ContractVersion.MAJOR + ".2");
        payload.remove("replaces");

        ItemLanded read = (ItemLanded) PayloadCodec.fromJson(payload);
        assertThat(read.replaces()).isNull();
        assertThat(read.fileId()).isEqualTo(sent.fileId());
        assertThat(read.storage()).isEqualTo(sent.storage());
    }

    /**
     * Another major version, newer or older, is refused, whatever else
     * the payload holds.
     */
    @Test
    void otherMajorVersionIsRefused()
    {
        for (String version : new String[] {
            (ContractVersion.MAJOR + 1) + ".0",
            (ContractVersion.MAJOR - 1) + "." + ContractVersion.MINOR})
        {
            assertThatThrownBy(() -> PayloadCodec.fromJson(
                    payload(landed(), version)))
                    .as(version)
                    .isInstanceOf(UnsupportedContractVersionException.class);
        }
        Map<String, Object> unmarked =
                new LinkedHashMap<>(PayloadCodec.toJson(landed()));
        unmarked.remove("v");
        assertThatThrownBy(() -> PayloadCodec.fromJson(unmarked))
                .isInstanceOf(UnsupportedContractVersionException.class);
    }

    /**
     * Sealed and opened as the splitter opens them: a later minor version
     * opens, and another major version is refused with
     * {@code UNSUPPORTED_VERSION}, so the splitter can tell it from a
     * message that is simply bad.
     *
     * @throws Exception if the test fails to run
     */
    @Test
    void openerTellsAnUnsupportedVersionFromABadMessage() throws Exception
    {
        KeyPair signer = ContractRoundTripTest.ec();
        KeyPair recipient = ContractRoundTripTest.ec();
        MessageSealer sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) signer.getPrivate()),
                "landing-2026", recipient.getPublic(), "inbox-2026");
        MessageOpener opener = new MessageOpener(
                Map.of("inbox-2026", recipient.getPrivate()),
                Map.of("landing-2026", (ECPublicKey) signer.getPublic()));

        ItemLanded sent = landed();
        Map<String, Object> later = payload(sent,
                ContractVersion.MAJOR + "." + (ContractVersion.MINOR + 1));
        later.put("priority", "high");
        assertThat(opener.open(sealer.seal(later))).isEqualTo(sent);

        byte[] nextMajor = sealer.seal(payload(sent,
                (ContractVersion.MAJOR + 1) + ".0"));
        assertThatThrownBy(() -> opener.open(nextMajor))
                .isInstanceOf(MessageRejectedException.class)
                .extracting(thrown ->
                        ((MessageRejectedException) thrown).reason())
                .isEqualTo(MessageRejectedException.Reason
                        .UNSUPPORTED_VERSION);

        Map<String, Object> broken = payload(sent, ContractVersion.CURRENT);
        broken.remove("fileId");
        assertThatThrownBy(() -> opener.open(sealer.seal(broken)))
                .isInstanceOf(MessageRejectedException.class)
                .extracting(thrown ->
                        ((MessageRejectedException) thrown).reason())
                .isEqualTo(MessageRejectedException.Reason.BAD_PAYLOAD);
    }
}
