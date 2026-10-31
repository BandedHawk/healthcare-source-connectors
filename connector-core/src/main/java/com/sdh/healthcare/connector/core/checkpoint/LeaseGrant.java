/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */

package com.sdh.healthcare.connector.core.checkpoint;

import java.time.Instant;

/**
 * A held lease. {@code epoch} is the fencing token; {@code expiresAt} is in the store's clock and is
 * informational only (the holder tracks validity with its own monotonic clock).
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
public record LeaseGrant(String leaseName, String owner, long epoch, Instant expiresAt) {}
