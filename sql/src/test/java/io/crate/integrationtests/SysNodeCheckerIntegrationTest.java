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
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class SysNodeCheckerIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testChecksPresenceAndSeverityLevels() throws Exception {
        SQLResponse response = execute("select id, severity, passed from sys.node_checks order by id, node_id asc");
        assertThat(response.rowCount(), equalTo(12L));
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
               "6| 3| true\n" +
               "7| 3| true\n" +   // 7 = FloodStageDiskWatermark
               "7| 3| true\n"));
    }

    @Test
    public void testUpdateAcknowledge() throws Exception {
        execute("update sys.node_checks set acknowledged = true where id = 3");
        assertThat(response.rowCount(), is(2L));

        execute("select acknowledged from sys.node_checks where id = 3");
        assertThat(response.rows()[0][0], is(true));
        assertThat(response.rows()[1][0], is(true));

        execute("update sys.node_checks set acknowledged = false where id = 3");
        assertThat(response.rowCount(), is(2L));

        execute("select acknowledged from sys.node_checks where id = 3");
        assertThat(response.rows()[0][0], is(false));
        assertThat(response.rows()[1][0], is(false));
    }

    @Test
    public void testUpdateAcknowledgedFromReference() throws Exception {
        execute("select count(*) from sys.node_checks where not passed");
        Long rc = (Long) response.rows()[0][0];
        assertThat(rc, greaterThan(0L));
        execute("update sys.node_checks set acknowledged = not passed where passed = false");
        assertThat(response.rowCount(), is(rc));
    }
}
