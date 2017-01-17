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

package io.crate.integrationtests;

import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
@UseJdbc
public class SysNodeCheckerIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testChecksPresenceAndSeverityLevels() throws Exception {
        SQLResponse response = execute("select id, severity, passed from sys.node_checks order by id, node_id asc");
        assertThat(response.rowCount(), equalTo(10L));
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("1| 3| false\n" +  // 1 = recoveryExpectedNodesCheck
               "1| 3| false\n" +
               "2| 3| false\n" +  // 2 = RecoveryAfterNodes
               "2| 3| false\n" +
               "3| 2| true\n" +   // 3 = RecoveryAfterTime
               "3| 2| true\n" +
               "5| 3| true\n" +   // 5 = HighDiskWatermark
               "5| 3| true\n" +
               "6| 3| true\n" +   // 6 = LowDiskWatermark
               "6| 3| true\n"));
    }

    @Test
    public void testUpdateAcknowledge() throws Exception {
        // gateway.expected_nodes is -1 in the test setup so this check always fails
        execute("select id, passed from sys.node_checks where passed = false");
        Object id = response.rows()[0][0];
        execute("update sys.node_checks set acknowledged = true where id = ?", new Object[]{id});

        execute("select id, passed, acknowledged from sys.node_checks where id = ?", new Object[]{id});
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("1| false| true\n" +
               "1| false| true\n"));
    }
}
