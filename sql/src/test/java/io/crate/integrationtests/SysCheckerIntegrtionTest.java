/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.integrationtests;

import io.crate.action.sql.SQLResponse;
import io.crate.operation.reference.sys.check.checks.SysCheck.Severity;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SysCheckerIntegrtionTest extends SQLTransportIntegrationTest {

    @Test
    public void testChecksPresenceAndSeverityLevels() throws Exception {
        SQLResponse response = execute("select severity, passed from sys.checks order by id asc");
        assertThat(response.rowCount(), equalTo(4L));
        assertThat((Integer) response.rows()[0][0], is(new Integer(Severity.HIGH.value())));
        assertThat((Integer) response.rows()[1][0], is(new Integer(Severity.HIGH.value())));
        assertThat((Integer) response.rows()[2][0], is(new Integer(Severity.HIGH.value())));
        assertThat((Integer) response.rows()[3][0], is(new Integer(Severity.MEDIUM.value())));
    }

    @Test
    public void testMinimumMasterNodesCheckSetNotCorrectNumberOfMasterNodes() throws InterruptedException {
        int setMinimumMasterNodes = internalCluster().size() / 2;
        execute("set global discovery.zen.minimum_master_nodes=?", new Object[]{setMinimumMasterNodes});
        SQLResponse response = execute("select severity, passed from sys.checks where id=?", new Object[]{1});
        assertThat((Integer) response.rows()[0][0], is(new Integer(Severity.HIGH.value())));
        assertThat((Boolean) response.rows()[0][1], is(new Boolean(false)));
    }

    @Test
    public void testMinimumMasterNodesCheckWithResetCorrectSetting() {
        int setMinimumMasterNodes = internalCluster().size() / 2 + 1;
        execute("set global discovery.zen.minimum_master_nodes=?", new Object[]{setMinimumMasterNodes});
        SQLResponse response = execute("select severity, passed from sys.checks where id=?", new Object[]{1});
        assertThat((Integer) response.rows()[0][0], is(new Integer(Severity.HIGH.value())));
        assertThat((Boolean) response.rows()[0][1], is(new Boolean(true)));
    }

    @After
    public void tearDown() throws Exception {
        execute("RESET GLOBAL discovery.zen.minimum_master_nodes");
        super.tearDown();
    }

}
