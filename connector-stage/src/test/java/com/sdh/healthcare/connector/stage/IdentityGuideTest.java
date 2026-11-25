/*
 * Copyright (c) 2026 Smile Digital Health Inc. All rights reserved.
 *
 * Proprietary and confidential. Use is subject to the terms in the LICENSE file
 * in the root of this project.
 */
package com.sdh.healthcare.connector.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.sdh.healthcare.connector.stage.admin.Permission;
import com.sdh.healthcare.connector.stage.control.ControlRoute;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;

/**
 * The identity and access document ({@code docs/security/IDENTITY.md},
 * HIPAA.md §11.2 D2) says what each permission allows, so a permission or a
 * control API operation added to the code can't be left out of what the
 * security official grants by.
 *
 * @author Jonas Barnett
 * @since 0.1.0
 */
class IdentityGuideTest
{

    /** The repository's root: the parent of the module's directory. */
    private static final Path ROOT = Path.of("").toAbsolutePath()
            .getParent();

    /** Creates the tests; JUnit runs each test on a new instance. */
    IdentityGuideTest()
    {
    }

    /**
     * Every permission is a row of §4.1, and every control API operation is
     * named there.
     *
     * @throws IOException if the document can't be read
     */
    @Test
    void namesEveryPermissionAndOperation() throws IOException
    {
        String guide = Files.readString(ROOT.resolve(
                "docs/security/IDENTITY.md"));
        int start = guide.indexOf("### 4.1");
        int end = guide.indexOf("### 4.2");
        assertThat(start).as("§4.1").isPositive();
        assertThat(end).as("§4.2").isGreaterThan(start);
        String table = guide.substring(start, end);
        Set<String> missing = new TreeSet<>();
        for (Permission permission : Permission.values())
        {
            if (!table.contains("\n| `" + permission.key() + "` |"))
            {
                missing.add(permission.key());
            }
        }
        for (ControlRoute route : ControlRoute.all())
        {
            if (!table.contains("`" + route.operation() + "`"))
            {
                missing.add(route.operation());
            }
        }
        assertThat(missing).as("permissions and operations §4.1 leaves out")
                .isEmpty();
    }
}
