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

import io.crate.operation.reference.sys.check.SysCheck.Severity;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(
    numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false, autoMinMasterNodes = false)
public class SysCheckerIntegrationTest extends SQLTransportIntegrationTest {

    @Test
    public void testChecksPresenceAndSeverityLevels() throws Exception {
        // must set minimum_master_nodes due to autoMinMasterNodes=false ClusterScope
        internalCluster().startNode(builder().put("discovery.zen.minimum_master_nodes", 1).build());
        internalCluster().ensureAtLeastNumDataNodes(1);

        SQLResponse response = execute("select severity, passed from sys.checks order by id asc");
        assertThat(response.rowCount(), equalTo(4L));
        assertThat(response.rows()[0][0], is(Severity.HIGH.value()));
        assertThat(response.rows()[1][0], is(Severity.MEDIUM.value()));
        assertThat(response.rows()[2][0], is(Severity.MEDIUM.value()));
    }

    @Test
    public void testMinimumMasterNodesCheckSetNotCorrectNumberOfMasterNodes() throws InterruptedException {
        Settings settings = builder().put("discovery.zen.minimum_master_nodes", 1).build();
        internalCluster().startNode(settings);
        internalCluster().startNode(settings);
        internalCluster().ensureAtLeastNumDataNodes(2);
        // update discovery.zen.minimum_master_nodes again:
        // test uses SUITE cluster-scope, so depending on the execution order of the tests there may be another node running
        // that has "minimum_master_nodes" set to something else - if the execute then hits that particular node
        // it would fail.
        execute("set global transient discovery.zen.minimum_master_nodes = 1");

        try {
            SQLResponse response = execute("select severity, passed from sys.checks where id=?", new Object[]{1});
            assertThat(TestingHelpers.printedTable(response.rows()), is("3| false\n"));
        } finally {
            execute("reset global discovery.zen.minimum_master_nodes");
        }
    }

    @Test
    public void testMinimumMasterNodesCheckWithCorrectSetting() {
        Settings settings = builder().put("discovery.zen.minimum_master_nodes", 1).build();
        internalCluster().startNode(settings);
        internalCluster().startNode(settings);
        internalCluster().ensureAtLeastNumDataNodes(2);
        execute("set global transient discovery.zen.minimum_master_nodes = 2");

        try {
            SQLResponse response = execute("select severity, passed from sys.checks where id=?", new Object[]{1});
            assertThat(TestingHelpers.printedTable(response.rows()), is("3| true\n"));
        } finally {
            execute("reset global discovery.zen.minimum_master_nodes");
        }
    }

    @Test
    public void testNumberOfPartitionCheckPassedForDocTablesCustomAndDefaultSchemas() {
        // must set minimum_master_nodes due to autoMinMasterNodes=false ClusterScope
        internalCluster().startNode(builder().put("discovery.zen.minimum_master_nodes", 1));
        internalCluster().ensureAtLeastNumDataNodes(1);
        execute("create table foo.bar (id int) partitioned by (id)");
        execute("create table bar (id int) partitioned by (id)");
        execute("insert into foo.bar (id) values (?)", new Object[]{1});
        execute("insert into bar (id) values (?)", new Object[]{1});
        SQLResponse response = execute("select severity, passed from sys.checks where id=?", new Object[]{2});
        assertThat(TestingHelpers.printedTable(response.rows()), is("2| true\n"));
    }
}
