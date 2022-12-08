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

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static io.crate.protocols.postgres.PGErrorStatus.DUPLICATE_TABLE;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import io.crate.exceptions.RelationAlreadyExists;
import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewsMetadata;
import io.crate.sql.Identifiers;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ViewsITest extends IntegTestCase {

    @After
    public void dropViews() {
        execute("SELECT pg_catalog.quote_ident(table_schema) || '.' || table_name FROM information_schema.views");
        if (response.rows().length > 0) {
            String views = Stream.of(response.rows())
                .map(row -> String.valueOf(row[0]))
                .collect(Collectors.joining(", "));
            execute(String.format("DROP VIEW %s", views));
        }
    }

    @Test
    public void testViewCanBeCreatedSelectedAndThenDropped() {
        execute("create table t1 (x int)");
        execute("insert into t1 (x) values (1)");
        execute("refresh table t1");
        execute("create view v1 as select * from t1 where x > ?", $(0));
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
            assertThat(views, Matchers.notNullValue());
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v1")), is(true));
        }
        assertThat(printedTable(execute("select * from v1").rows()), is("1\n"));
        assertThat(
            printedTable(execute("select view_definition from information_schema.views").rows()),
            is("SELECT *\nFROM \"t1\"\nWHERE \"x\" > 0\n\n")
        );
        execute("drop view v1");
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v1")), is(false));
        }
    }

    @Test
    public void testViewCanBeUsedForJoins() {
        execute("CREATE TABLE t1 (a STRING, x INTEGER)");
        execute("INSERT INTO t1 (x, a) VALUES (1, 'foo')");
        execute("REFRESH TABLE t1");
        execute("CREATE VIEW v1 AS select * FROM t1");
        execute("CREATE VIEW v2 AS select * FROM t1");
        assertThat(printedTable(execute("SELECT * FROM v1 INNER JOIN v2 ON v1.x = v2.x").rows()), is("foo| 1| foo| 1\n"));
    }

    @Test
    public void testViewCanBeCreatedAndThenReplaced() {
        execute("create view v2 as select 1 from sys.cluster");
        assertThat(printedTable(execute("select * from v2").rows()), is("1\n"));
        execute("create or replace view v2 as select 2 from sys.cluster");
        assertThat(printedTable(execute("select * from v2").rows()), is("2\n"));
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            ViewsMetadata views = clusterService.state().metadata().custom(ViewsMetadata.TYPE);
            assertThat(views, Matchers.notNullValue());
            assertThat(views.contains(RelationName.fromIndexName(sqlExecutor.getCurrentSchema() + ".v2")), is(true));
        }
    }

    @Test
    public void testCreateViewFailsIfViewAlreadyExists() {
        execute("create view v3 as select 1");

        assertThrowsMatches(() -> execute("create view v3 as select 1"),
                            isSQLError(containsString(
                                           "Relation '" + sqlExecutor.getCurrentSchema() + ".v3' already exists"),
                                       DUPLICATE_TABLE,
                                       CONFLICT,
                                       4093));
    }

    @Test
    public void testCreateViewFailsIfNameConflictsWithTable() {
        execute("create table t1 (x int) clustered into 1 shards with (number_of_replicas = 0)");

        assertThrowsMatches(() -> execute("create view t1 as select 1"),
                     isSQLError(containsString("Relation '" + sqlExecutor.getCurrentSchema() + ".t1' already exists"),
                                DUPLICATE_TABLE,
                                CONFLICT,
                                4093));
    }

    @Test
    public void testCreateViewFailsIfNameConflictsWithPartitionedTable() {
        execute("create table t1 (x int) partitioned by (x) clustered into 1 shards with (number_of_replicas = 0)");

        assertThrowsMatches(() -> execute("create view t1 as select 1"),
                     isSQLError(containsString("Relation '" + sqlExecutor.getCurrentSchema() + ".t1' already exists"),
                                DUPLICATE_TABLE,
                                CONFLICT,
                                4093));
    }

    @Test
    public void testCreateTableFailsIfNameConflictsWithView() {
        // First plan the create table which should conflict with the view,
        PlanForNode viewConflictingTableCreation =
            plan("create table v4 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        // then create the actual view. This way we circumvent the analyzer check for existing views.
        execute("create view v4 as select 1");

        Assertions.assertThrows(RelationAlreadyExists.class,
                                () -> execute(viewConflictingTableCreation).getResult(),
                                "Relation '" + sqlExecutor.getCurrentSchema() + ".v4' already exists"
        );
    }

    @Test
    public void testCreatePartitionedTableFailsIfNameConflictsWithView() {
        // First plan the create table which should conflict with the view,
        PlanForNode viewConflictingTableCreation =
            plan("create table v5 (x int) clustered into 1 shards with (number_of_replicas = 0)");
        // then create the actual view. This way we circumvent the analyzer check for existing views.
        execute("create view v5 as select 1");

        Assertions.assertThrows(RelationAlreadyExists.class,
                                () -> execute(viewConflictingTableCreation).getResult(),
                                "Relation '" + sqlExecutor.getCurrentSchema() + ".v5' already exists"
        );
    }

    @Test
    public void testDropViewFailsIfViewIsMissing() {
        var schema = sqlExecutor.getCurrentSchema();
        assertThrowsMatches(() -> execute("drop view v1"),
                     isSQLError(containsString("Relations not found: " + Identifiers.quoteIfNeeded(schema) + ".v1"),
                                INTERNAL_ERROR,
                                NOT_FOUND,
                                4041));
    }

    @Test
    public void testDropViewDoesNotFailIfViewIsMissingAndIfExistsIsUsed() {
        execute("drop view if exists v1");
    }

    @Test
    public void testSubscriptOnViews() {
        execute("create table t1 (a object as (b integer), c object as (d object as (e integer))) ");
        execute("insert into t1 (a, c) values ({ b = 1 }, { d = { e = 2 }})");
        execute("refresh table t1");
        execute("create view v1 as select * from t1");
        // must not throw an exception, subscript must be resolved
        execute("select a['b'], c['d']['e'] from v1");
        assertThat(printedTable(response.rows()), is("1| 2\n"));
    }

    @Test
    public void test_where_clause_on_view_normalized_on_coordinator_node() {
        execute("create table test (x timestamp, y int)");
        execute("create view v_test as select * from test");
        execute("select * from v_test where x > current_timestamp - INTERVAL '24' HOUR");
    }


    @Test
    public void test_creating_a_self_referencing_view_is_not_allowed() {
        execute("create view v as select * from sys.cluster");
        assertThrowsMatches(
            () -> execute("create or replace view v as select * from v"),
            isSQLError(
                containsString("Creating a view that references itself is not allowed"),
                INTERNAL_ERROR,
                HttpResponseStatus.BAD_REQUEST,
                4000
            )
        );
    }
}
