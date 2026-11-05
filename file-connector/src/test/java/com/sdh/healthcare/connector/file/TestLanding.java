/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.file;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.nimbusds.jose.crypto.ECDSASigner;
import com.sdh.healthcare.connector.contract.crypto.KeyProvider;
import com.sdh.healthcare.connector.contract.jose.MessageSealer;
import com.sdh.healthcare.connector.contract.message.ConnectorMessage;
import com.sdh.healthcare.connector.core.checkpoint.FencedCommitter;
import com.sdh.healthcare.connector.core.checkpoint.LeaseToken;
import com.sdh.healthcare.connector.core.crypto.IdKeys;
import com.sdh.healthcare.connector.core.crypto.SectionIds;
import com.sdh.healthcare.connector.core.landing.ItemKind;
import com.sdh.healthcare.connector.core.landing.ItemLander;
import com.sdh.healthcare.connector.core.landing.LandedItem;
import com.sdh.healthcare.connector.core.landing.LandedItems;
import com.sdh.healthcare.connector.core.landing.LandingTables;
import com.sdh.healthcare.connector.core.landing.NewItem;
import com.sdh.healthcare.connector.core.pipeline.UuidV7;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.interfaces.ECPrivateKey;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.CamelContext;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;

/**
 * Stands in for a landing node in the splitter's tests: lands items in the
 * landing namespace of a catalog, as a node holding epoch 1, and sends
 * their {@code item.landed} messages, sealed to the splitter and signed
 * with the landing key.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public final class TestLanding
{

    /** The landing catalog's name, as the splitter is set up with. */
    public static final String CATALOG = "sdh";

    /** The {@value LandingTables#LANDED_ITEMS} table. */
    private final Table items;

    /** Unwraps the items' data keys. */
    private final KeyProvider wrap;

    /** The landing ID key, for digests. */
    private final SectionIds ids;

    /** Lands items. */
    private final ItemLander lander;

    /** Seals messages as the landing node. */
    private final MessageSealer sealer;

    /**
     * Opens the landing tables.
     *
     * @param catalogProperties the catalog's properties
     * @param wrap              wraps data keys with the landed-items KEK
     * @param keys              the test's key files
     * @param intake            the intake items land through
     * @throws Exception if the landing signing key can't be used
     */
    public TestLanding(Map<String, String> catalogProperties,
            KeyProvider wrap, TestKeys keys, String intake) throws Exception
    {
        Catalog catalog = CatalogUtil.buildIcebergCatalog(CATALOG,
                catalogProperties, null);
        this.wrap = wrap;
        items = LandingTables.landedItems(catalog,
                LandingTables.DEFAULT_NAMESPACE);
        FencedCommitter committer = new FencedCommitter(new LeaseToken()
        {
            @Override
            public long epoch()
            {
                return 1;
            }

            @Override
            public void checkHeld()
            {
            }
        });
        committer.establish(items);
        ids = new SectionIds(IdKeys.loadOrCreate(items, committer,
                wrap, TestKeys.LANDED));
        lander = new ItemLander(items, committer, wrap, ids,
                ItemLander.Settings.defaults(intake, TestKeys.LANDED),
                Clock.systemUTC());
        sealer = new MessageSealer(
                new ECDSASigner((ECPrivateKey) keys.landingSigning
                        .getPrivate()),
                TestKeys.LANDING_SIGNER_KID, keys.inbox.getPublic(),
                TestKeys.INBOX_KID);
    }

    /**
     * Lands one item.
     *
     * @param content      its bytes, as text
     * @param originalName its original name; may be null
     * @return the landed item
     * @throws Exception if it can't be landed
     */
    public LandedItem land(String content, String originalName)
            throws Exception
    {
        return land(content.getBytes(UTF_8), originalName);
    }

    /**
     * Lands one item, byte for byte.
     *
     * @param bytes        its bytes
     * @param originalName its original name; may be null
     * @return the landed item
     * @throws Exception if it can't be landed
     */
    public LandedItem land(byte[] bytes, String originalName)
            throws Exception
    {
        return lander.land(new NewItem(UuidV7.next(System.currentTimeMillis()),
                ItemKind.FILE, null, originalName, null),
                offset -> new ByteArrayInputStream(bytes, (int) offset,
                        bytes.length - (int) offset));
    }

    /**
     * Lands one item from a file, read as a stream, so that an item larger
     * than the heap can be landed.
     *
     * @param file         the file
     * @param originalName its original name; may be null
     * @return the landed item
     * @throws Exception if it can't be landed
     */
    public LandedItem land(Path file, String originalName) throws Exception
    {
        return lander.land(new NewItem(UuidV7.next(System.currentTimeMillis()),
                ItemKind.FILE, null, originalName, null), offset ->
                {
                    InputStream input = Files.newInputStream(file);
                    input.skipNBytes(offset);
                    return input;
                });
    }

    /**
     * Reads a landed item back, checking its digest.
     *
     * @param itemId the item
     * @return its bytes
     * @throws IOException if it can't be read or is damaged
     */
    public byte[] restore(UUID itemId) throws IOException
    {
        items.refresh();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        LandedItems.read(items, itemId, wrap, ids, output);
        return output.toByteArray();
    }

    /**
     * Seals a message as the landing node.
     *
     * @param message the message
     * @return the sealed message
     */
    public byte[] seal(ConnectorMessage message)
    {
        return sealer.seal(message);
    }

    /**
     * Seals a payload as the landing node, as given: for payloads this
     * build wouldn't write, such as one of another contract version.
     *
     * @param payload the payload's fields
     * @return the sealed message
     */
    public byte[] seal(Map<String, Object> payload)
    {
        return sealer.seal(payload);
    }

    /**
     * An item's {@code item.landed} message, sealed.
     *
     * @param item the item
     * @return the sealed message
     */
    public byte[] announcement(LandedItem item)
    {
        return seal(item.message(CATALOG, LandingTables.DEFAULT_NAMESPACE));
    }

    /**
     * Sends an item's {@code item.landed} message.
     *
     * @param camel    the splitter's Camel context
     * @param endpoint where the splitter consumes from
     * @param item     the item
     */
    public void announce(CamelContext camel, String endpoint, LandedItem item)
    {
        camel.createProducerTemplate().sendBody(endpoint, announcement(item));
    }
}
