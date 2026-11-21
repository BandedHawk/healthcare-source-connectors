/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage.admin;

import java.util.Map;

/**
 * An operation the control API serves (CONTROL-API-DESIGN.md §5): its
 * result as the API answers it, field by field. The answer is written out
 * by hand, never by reflection over the result, so a field the CLI never
 * shows (a sealed reason, a record's content) can't reach a caller by
 * being added to a record.
 *
 * @param <R> the operation's result
 * @author Jonas Barnett
 * @since 0.1.0
 */
public interface Answering<R>
{

    /**
     * The result, as the API answers it.
     *
     * @param result the result
     * @return its fields: text, numbers, booleans, nulls, lists and maps
     *         of them only, built with {@link Answers}
     */
    Map<String, Object> answer(R result);
}
