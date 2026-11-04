/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.landing.config;

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * An Iceberg table intake's settings, checked (design §5.3.18).
 *
 * @param catalog     the catalog's properties
 * @param catalogName the catalog's name
 * @param identifier  the table
 * @param earliest    whether the first start lands every row the table
 *                    holds, rather than only later appends
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record TableSettings(Map<String, String> catalog, String catalogName,
        TableIdentifier identifier, boolean earliest)
{

    /** The settings' common prefix. */
    private static final String PREFIX = "sdh.landing.source.table.";

    /**
     * Checks a table's settings.
     *
     * @param table the settings as bound
     * @return the checked settings
     * @throws IllegalArgumentException if a setting is missing or unknown
     */
    public static TableSettings of(LandingProperties.Table table)
    {
        if (table == null || table.catalog() == null
                || table.catalog().isEmpty())
        {
            throw new IllegalArgumentException(PREFIX + "catalog is required");
        }
        if (!table.catalog().containsKey("type")
                && !table.catalog().containsKey("catalog-impl"))
        {
            throw new IllegalArgumentException(PREFIX
                    + "catalog needs a type or a catalog-impl");
        }
        String name = SmbSettings.required(table.catalogName(),
                PREFIX + "catalog-name");
        String identifier = SmbSettings.required(table.identifier(),
                PREFIX + "identifier");
        TableIdentifier parsed = TableIdentifier.parse(identifier.trim());
        if (!parsed.hasNamespace())
        {
            throw new IllegalArgumentException(PREFIX
                    + "identifier must be namespace.table");
        }
        boolean earliest = switch (table.start() == null ? ""
                : table.start().trim().toLowerCase(Locale.ROOT))
        {
            case "earliest" -> true;
            case "latest" -> false;
            default -> throw new IllegalArgumentException(
                    PREFIX + "start must be earliest or latest");
        };
        return new TableSettings(Map.copyOf(table.catalog()), name, parsed,
                earliest);
    }

    /**
     * Shows the table without the catalog's properties, which may hold a
     * secret.
     *
     * @return the table's name
     */
    @Override
    public String toString()
    {
        return "TableSettings[" + catalogName + ":" + identifier + "]";
    }
}
