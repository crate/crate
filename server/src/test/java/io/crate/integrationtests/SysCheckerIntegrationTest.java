/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.expression.reference.sys.check.SysCheck.Severity;
import io.crate.testing.SQLResponse;
import io.crate.testing.TestingHelpers;

@IntegTestCase.ClusterScope(
    numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false)
public class SysCheckerIntegrationTest extends IntegTestCase {

    @Test
    public void testChecksPresenceAndSeverityLevels() throws Exception {
        cluster().startNode(cluster().getDefaultSettings());
        cluster().ensureAtLeastNumDataNodes(1);

        SQLResponse response = execute("select severity, passed from sys.checks order by id asc");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo(Severity.MEDIUM.value());
        assertThat(response.rows()[1][0]).isEqualTo(Severity.LOW.value());
    }

    @Test
    public void testNumberOfPartitionCheckPassedForDocTablesCustomAndDefaultSchemas() {
        cluster().startNode();
        cluster().ensureAtLeastNumDataNodes(1);
        execute("create table foo.bar (id int) partitioned by (id)");
        execute("create table bar (id int) partitioned by (id)");
        execute("insert into foo.bar (id) values (?)", new Object[]{1});
        execute("insert into bar (id) values (?)", new Object[]{1});
        SQLResponse response = execute("select severity, passed from sys.checks where id=?", new Object[]{2});
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("2| true\n");
    }

    @Test
    public void testSelectingConcurrentlyFromSysCheckPassesWithoutExceptions() throws Exception {
        cluster().startNode();
        cluster().startNode();
        cluster().ensureAtLeastNumDataNodes(2);

        List<CompletableFuture<SQLResponse>> responses = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            responses.add(sqlExecutor.execute("select * from sys.checks", null));
        }
        for (var response : responses) {
            response.get(5, TimeUnit.SECONDS);
        }
    }
}
