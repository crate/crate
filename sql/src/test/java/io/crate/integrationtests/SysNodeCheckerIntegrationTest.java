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

import io.crate.action.sql.SQLResponse;
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class SysNodeCheckerIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testChecksPresenceAndSeverityLevels() throws Exception {
        SQLResponse response = execute("select severity, passed from sys.node_checks order by node_id, id asc");
        assertThat(response.rowCount(), equalTo(8L));
        assertThat((Integer) response.rows()[0][0], is(SysCheck.Severity.HIGH.value()));
        assertThat((Integer) response.rows()[1][0], is(SysCheck.Severity.HIGH.value()));
        assertThat((Integer) response.rows()[2][0], is(SysCheck.Severity.MEDIUM.value()));
        assertThat((Integer) response.rows()[3][0], is(SysCheck.Severity.MEDIUM.value()));
    }

    @Test
    public void testUpdateAcknowledge() throws Exception {
        // gateway.expected_nodes is -1 in the test setup so this test always fails
        SQLResponse resp = execute("select id, passed from sys.node_checks where passed = false");
        execute("update sys.node_checks set acknowledged = true where id = ?", resp.rows()[0]);

        execute("select id, passed, acknowledged from sys.node_checks where id = ?", resp.rows()[0]);
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("1| false| true\n" +
               "1| false| true\n"));
    }
}
