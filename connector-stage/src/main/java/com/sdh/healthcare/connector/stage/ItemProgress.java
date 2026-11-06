/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import java.util.UUID;

/**
 * An item whose records have all been stored by a stage (STAGES.md §5).
 *
 * @param itemId   the item's id
 * @param expected how many records it has
 * @param done     how many the stage stored, failed ones included
 * @param failed   how many of those failed
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record ItemProgress(UUID itemId, long expected, long done,
        long failed)
{
}
