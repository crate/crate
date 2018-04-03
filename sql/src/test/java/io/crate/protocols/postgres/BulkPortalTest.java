/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.postgres;

import io.crate.action.sql.SessionContext;
import io.crate.sql.tree.Statement;
import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class BulkPortalTest extends CrateUnitTest {

    @Test
    public void testPortalDoesNotSupportDifferentStatements() {
        String query = "select * from t";
        BulkPortal p1 = new BulkPortal("P1",
            query,
            Mockito.mock(Statement.class),
            null,
            Collections.emptyList(),
            Collections.emptyList(),
            Mockito.mock(ResultSetReceiver.class),
            1,
            Collections.emptyList(),
            SessionContext.create(),
            Mockito.mock(AbstractPortal.PortalContext.class)
        );

        p1.bind("S1",
            query,
            Mockito.mock(Statement.class),
            null,
            Collections.emptyList(),
            null
        );

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Bulk portal doesn't expect the query to change.\n" +
                                        "Old query: select * from t\n" +
                                        "New query: QUERY CHANGED");

        p1.bind("S2",
            "QUERY CHANGED",
            Mockito.mock(Statement.class),
            null,
            Collections.emptyList(),
            null
        );

    }
}
