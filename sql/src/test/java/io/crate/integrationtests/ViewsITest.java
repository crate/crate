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

import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewsMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

public class ViewsITest extends SQLTransportIntegrationTest {

    @After
    public void dropViews() {
        execute("SELECT table_schema || '.' || table_name FROM information_schema.views");
        if (response.rows().length > 0) {
            String views = Stream.of(response.rows())
                .map(row -> String.valueOf(row[0]))
                .collect(Collectors.joining(", "));
            execute(String.format("DROP VIEW %s", views));
        }
    }

    @Test
    public void testViewCanBeCreatedSelectedAndThenDropped() throws Exception {
        execute("create table t1 (x int)");
        execute("insert into t1 (x) values (1)");
        execute("refresh table t1");
        execute("create view v1 as select * from t1");
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetaData views = clusterService.state().metaData().custom(ViewsMetaData.TYPE);
            assertThat(views, Matchers.notNullValue());
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v1")), is(true));
        }
        assertThat(printedTable(execute("select * from v1").rows()), is("1\n"));
        execute("drop view v1");
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetaData views = clusterService.state().metaData().custom(ViewsMetaData.TYPE);
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v1")), is(false));
        }
    }

    @Test
    public void testViewCanBeUsedForJoins() {
        execute("CREATE TABLE t1 (x INTEGER, a STRING)");
        execute("INSERT INTO t1 (x, a) VALUES (1, 'foo')");
        execute("REFRESH TABLE t1");
        execute("CREATE VIEW v1 AS select * FROM t1");
        execute("CREATE VIEW v2 AS select * FROM t1");
        assertThat(printedTable(execute("SELECT * FROM v1 INNER JOIN v2 ON v1.x = v2.x").rows()), is("foo| 1| foo| 1\n"));
    }

    @Test
    public void testViewCanBeCreatedAndThenReplaced() throws Exception {
        execute("create view v2 as select 1 from sys.cluster");
        assertThat(printedTable(execute("select * from v2").rows()), is("1\n"));
        execute("create or replace view v2 as select 2 from sys.cluster");
        assertThat(printedTable(execute("select * from v2").rows()), is("2\n"));
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetaData views = clusterService.state().metaData().custom(ViewsMetaData.TYPE);
            assertThat(views, Matchers.notNullValue());
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v2")), is(true));
        }
    }

    @Test
    public void testCreateViewFailsIfViewAlreadyExists() {
        execute("create view v3 as select 1");

        expectedException.expectMessage("Relation '" + sqlExecutor.getCurrentSchema() + ".v3' already exists");
        execute("create view v3 as select 1");
    }

    @Test
    public void testCreateViewFailsIfNameConflictsWithTable() {
        execute("create table t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");

        expectedException.expectMessage("Relation '" + sqlExecutor.getCurrentSchema() + ".t1' already exists");
        execute("create view t1 as select 1");
    }

    @Test
    public void testCreateViewFailsIfNameConflictsWithPartitionedTable() {
        execute("create table t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");
        expectedException.expectMessage("Relation '" + sqlExecutor.getCurrentSchema() + ".t1' already exists");
        execute("create view t1 as select 1");
    }

    @Test
    public void testCreateTableFailsIfNameConflictsWithView() throws Exception {
        // First plan the create table which should conflict with the view,
        PlanForNode viewConflictingTableCreation =
            plan("create table v4 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        // then create the actual view. This way we circumvent the analyzer check for existing views.
        execute("create view v4 as select 1");

        expectedException.expectMessage("Relation '" + sqlExecutor.getCurrentSchema() + ".v4' already exists");
        execute(viewConflictingTableCreation).getResult();
    }

    @Test
    public void testCreatePartitionedTableFailsIfNameConflictsWithView() throws Exception {
        // First plan the create table which should conflict with the view,
        PlanForNode viewConflictingTableCreation =
            plan("create table v5 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        // then create the actual view. This way we circumvent the analyzer check for existing views.
        execute("create view v5 as select 1");

        expectedException.expectMessage("Relation '" + sqlExecutor.getCurrentSchema() + ".v5' already exists");
        execute(viewConflictingTableCreation).getResult();
    }

    @Test
    public void testDropViewFailsIfViewIsMissing() {
        expectedException.expectMessage("Relations not found: " + sqlExecutor.getCurrentSchema() + ".v1");
        execute("drop view v1");
    }

    @Test
    public void testDropViewDoesNotFailIfViewIsMissingAndIfExistsIsUsed() {
        execute("drop view if exists v1");
    }
}
