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
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_TABLE;
import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.Asserts;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseNewCluster;
import io.crate.testing.UseRandomizedOptimizerRules;
import io.crate.testing.UseRandomizedSchema;
import io.netty.handler.codec.http.HttpResponseStatus;

@UseRandomizedOptimizerRules(0)
@IntegTestCase.ClusterScope()
@UseRandomizedSchema(random = false)
public class DDLIntegrationTest extends IntegTestCase {

    @Test
    @UseNewCluster
    public void testCreateTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) " +
                "clustered into 5 shards with (number_of_replicas = 1, \"write.wait_for_active_shards\"=1)");
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{" +
                                 "\"primary_keys\":[\"col1\"]}," +
                                 "\"properties\":{" +
                                 // doc_values: true is default and not included
                                 "\"col1\":{\"type\":\"integer\",\"position\":1,\"oid\":1}," +
                                 "\"col2\":{\"type\":\"keyword\",\"position\":2,\"oid\":2}" +
                                 "}}}";

        String expectedSettings = "{\"test\":{" +
                                  "\"settings\":{" +
                                  "\"index.number_of_replicas\":\"1\"," +
                                  "\"index.number_of_shards\":\"5\"," +
                                  "\"index.version.created\":\"" + Version.CURRENT.internalId + "\"" +
                                  "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);

        // test index usage
        execute("insert into test (col1, col2) values (1, 'foo')");
        assertEquals(1, response.rowCount());
        refresh();
        execute("SELECT * FROM test");
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void testCreateTableWithRefreshIntervalDisableRefresh() throws Exception {
        execute("create table test (id int primary key, content string) " +
                "clustered into 5 shards " +
                "with (refresh_interval=0, number_of_replicas = 0)");
        String expectedSettings = "{\"test\":{" +
                                  "\"settings\":{" +
                                  "\"index.number_of_replicas\":\"0\"," +
                                  "\"index.number_of_shards\":\"5\"," +
                                  "\"index.refresh_interval\":\"0s\"," +
                                  "\"index.version.created\":\"" + Version.CURRENT.internalId + "\"" +
                                  "}}}";
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);

        execute("ALTER TABLE test SET (refresh_interval = '5000ms')");
        String expectedSetSettings = "{\"test\":{" +
                                     "\"settings\":{" +
                                     "\"index.number_of_replicas\":\"0\"," +
                                     "\"index.number_of_shards\":\"5\"," +
                                     "\"index.refresh_interval\":\"5s\"," +
                                     "\"index.version.created\":\"" + Version.CURRENT.internalId + "\"" +
                                     "}}}";
        JSONAssert.assertEquals(expectedSetSettings, getIndexSettings("test"), false);

        execute("ALTER TABLE test RESET (refresh_interval)");
        String expectedResetSettings = "{\"test\":{" +
                                       "\"settings\":{" +
                                       "\"index.number_of_replicas\":\"0\"," +
                                       "\"index.number_of_shards\":\"5\"," +
                                       "\"index.refresh_interval\":\"1s\"," +
                                       "\"index.version.created\":\"" + Version.CURRENT.internalId + "\"" +
                                       "}}}";
        JSONAssert.assertEquals(expectedResetSettings, getIndexSettings("test"), false);
    }


    @Test
    public void testCreateTableAlreadyExistsException() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        Asserts.assertSQLError(() -> execute("create table test (col1 integer primary key, col2 string)"))
            .hasMessageContaining("Relation 'doc.test' already exists.")
            .hasPGError(DUPLICATE_TABLE)
            .hasHTTPError(CONFLICT, 4093);
    }

    @Test
    @UseNewCluster
    public void testCreateTableWithReplicasAndShards() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)" +
                "clustered by (col1) into 10 shards with (number_of_replicas=2, \"write.wait_for_active_shards\"=1)");
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\"," +
                                 "\"_meta\":{" +
                                 "\"routing\":\"col1\"," +
                                 "\"primary_keys\":[\"col1\"]}," +
                                 "\"properties\":{" +
                                 "\"col1\":{\"type\":\"integer\",\"position\":1,\"oid\":1}," +
                                 "\"col2\":{\"type\":\"keyword\",\"position\":2,\"oid\":2}" +
                                 "}}}";

        String expectedSettings = "{\"test\":{" +
                                  "\"settings\":{" +
                                  "\"index.number_of_replicas\":\"2\"," +
                                  "\"index.number_of_shards\":\"10\"," +
                                  "\"index.version.created\":\"" + Version.CURRENT.internalId + "\"" +
                                  "}}}";

        JSONAssert.assertEquals(expectedMapping, getIndexMapping("test"), JSONCompareMode.LENIENT);
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), JSONCompareMode.LENIENT);
    }

    @Test
    @UseNewCluster
    public void testCreateTableWithStrictColumnPolicy() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) " +
                "clustered into 5 shards " +
                "with (column_policy='strict', number_of_replicas = 0)");
        String expectedMapping = "{\"default\":{" +
            "\"dynamic\":\"strict\"," +
            "\"_meta\":{\"primary_keys\":[\"col1\"]}," +
            "\"properties\":{" +
            "\"col1\":{\"type\":\"integer\",\"position\":1,\"oid\":1}," +
            "\"col2\":{\"type\":\"keyword\",\"position\":2,\"oid\":2}}}}";

        String expectedSettings = "{\"test\":{" +
                                  "\"settings\":{" +
                                  "\"index.number_of_replicas\":\"0\"," +
                                  "\"index.number_of_shards\":\"5\"," +
                                  "\"index.version.created\":\"" + Version.CURRENT.internalId + "\"" +
                                  "}}}";


        assertEquals(expectedMapping, getIndexMapping("test"));
        JSONAssert.assertEquals(expectedSettings, getIndexSettings("test"), false);
    }

    @Test
    @UseNewCluster
    public void testCreateGeoShapeExplicitIndex() throws Exception {
        execute("create table test (col1 geo_shape INDEX using QUADTREE with (precision='1m', distance_error_pct='0.25'))");
        ensureYellow();
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{}," +
                                 "\"properties\":{" +
                                 "\"col1\":{\"type\":\"geo_shape\",\"tree\":\"quadtree\",\"position\":1,\"oid\":1,\"precision\":\"1.0m\",\"distance_error_pct\":0.25}}}}";
        assertEquals(expectedMapping, getIndexMapping("test"));
    }

    @Test
    @UseNewCluster
    public void testCreateColumnWithDefaultExpression() throws Exception {
        execute("create table test (id int, col1 text default 'foo', col2 int[] default [1,2])");
        String expectedMapping = "{\"default\":{" +
            "\"dynamic\":\"strict\"," +
            "\"_meta\":{}," +
            "\"properties\":{" +
            "\"id\":{\"type\":\"integer\",\"position\":1,\"oid\":1}," +
            "\"col1\":{\"type\":\"keyword\",\"position\":2,\"default_expr\":\"'foo'\",\"oid\":2}," +
            "\"col2\":{\"type\":\"array\",\"inner\":{\"type\":\"integer\",\"position\":3,\"default_expr\":\"[1, 2]\",\"oid\":3}}" +
            "}}}";
        assertThat(getIndexMapping("test")).isEqualTo(expectedMapping);
        execute("insert into test(id) values(1)");
        execute("refresh table test");
        execute("select id, col1, col2 from test");
        assertThat(response).hasRows("1| foo| [1, 2]");
    }

    @Test
    @UseNewCluster
    public void testCreateGeoShape() throws Exception {
        execute("create table test (col1 geo_shape)");
        ensureYellow();
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{}," +
                                 "\"properties\":{\"col1\":{\"type\":\"geo_shape\",\"position\":1,\"oid\":1}}}}";
        assertEquals(expectedMapping, getIndexMapping("test"));

    }

    @Test
    public void testGeoShapeInvalidPrecision() throws Exception {
        Asserts.assertSQLError(() -> execute("create table test (col1 geo_shape INDEX using QUADTREE with (precision='10%'))"))
            .hasMessageContaining("Value '10%' of setting precision is not a valid distance unit")
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000);
    }

    @Test
    public void testGeoShapeInvalidDistance() throws Exception {
        Asserts.assertSQLError(() -> execute(
            "create table test (col1 geo_shape INDEX using QUADTREE with (distance_error_pct=true))"))
            .hasMessageContaining("Value 'true' of setting distance_error_pct is not a float value")
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000);
    }

    @Test
    public void testUnknownGeoShapeSetting() throws Exception {
        Asserts.assertSQLError(() -> execute("create table test (col1 geo_shape INDEX using QUADTREE with (does_not_exist=false))"))
            .hasMessageContaining("Setting \"does_not_exist\" ist not supported on geo_shape index")
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000);
    }

    @Test
    public void testCreateTableWithInlineDefaultIndex() throws Exception {
        execute("create table quotes (quote string index using plain) with (number_of_replicas = 0)");
        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        refresh();

        // matching does not work on plain indexes
        execute("select quote from quotes where match(quote, 'time')");
        assertEquals(0, response.rowCount());

        // filtering on the actual value does work
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);
    }

    @Test
    public void testCreateTableWithInlineIndex() throws Exception {
        execute("create table quotes (quote string index using fulltext) with (number_of_replicas = 0)");
        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        execute("refresh table quotes");

        execute("select quote from quotes where match(quote, 'time')");
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);

        // filtering on the actual value does not work anymore because its now indexed using the
        // standard analyzer
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(0, response.rowCount());
    }

    @Test
    public void testCreateTableWithIndexOff() throws Exception {
        execute("create table quotes (id int, quote string index off) with (number_of_replicas = 0)");
        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes (id, quote) values (?, ?)", new Object[]{1, quote});
        execute("refresh table quotes");
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertThat(response).hasRows(quote);
    }

    @Test
    public void testCreateTableWithIndex() throws Exception {
        execute("create table quotes (quote string, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='stop')) with (number_of_replicas = 0)");
        String quote = "Would it save you a lot of time if I just gave up and went mad now?";
        execute("insert into quotes values (?)", new Object[]{quote});
        execute("refresh table quotes");

        execute("select quote from quotes where match(quote_fulltext, 'time')");
        assertEquals(1L, response.rowCount());
        assertEquals(quote, response.rows()[0][0]);

        // filtering on the actual value does still work
        execute("select quote from quotes where quote = ?", new Object[]{quote});
        assertEquals(1L, response.rowCount());
    }

    @Test
    @UseNewCluster
    public void testCreateTableWithCompositeIndex() throws Exception {
        execute("create table novels (title string, description string, " +
                "index title_desc_fulltext using fulltext(title, description) " +
                "with(analyzer='stop')) with (number_of_replicas = 0)");

        String expectedMapping = "{\"default\":{" +
            "\"dynamic\":\"strict\",\"" +
            "_meta\":{\"indices\":{\"3\":{}}}," +
            "\"properties\":{" +
            "\"title\":{\"type\":\"keyword\",\"position\":1,\"oid\":1}," +
            "\"description\":{\"type\":\"keyword\",\"position\":2,\"oid\":2}," +
            "\"title_desc_fulltext\":{\"type\":\"text\",\"position\":3,\"oid\":3,\"analyzer\":\"stop\",\"sources\":[\"1\",\"2\"]}}}}";
        assertEquals(expectedMapping, getIndexMapping("novels"));

        String title = "So Long, and Thanks for All the Fish";
        String description = "Many were increasingly of the opinion that they'd all made a big " +
                             "mistake in coming down from the trees in the first place. And some said that " +
                             "even the trees had been a bad move, and that no one should ever have left " +
                             "the oceans.";
        execute("insert into novels (title, description) values(?, ?)",
            new Object[]{title, description});
        refresh();

        // match token existing at field `title`
        execute("select title, description from novels where match(title_desc_fulltext, 'fish')");
        assertEquals(1L, response.rowCount());
        assertEquals(title, response.rows()[0][0]);
        assertEquals(description, response.rows()[0][1]);

        // match token existing at field `description`
        execute("select title, description from novels where match(title_desc_fulltext, 'oceans')");
        assertEquals(1L, response.rowCount());
        assertEquals(title, response.rows()[0][0]);
        assertEquals(description, response.rows()[0][1]);

        // filtering on the actual values does still work
        execute("select title from novels where title = ?", new Object[]{title});
        assertEquals(1L, response.rowCount());
    }

    @Test
    public void test_create_table_with_check_fail_on_insert() {
        execute("create table t (id integer primary key, qty integer, constraint check_1 check (qty > 0))");
        execute("insert into t(id, qty) values(0, null), (1, 1)");
        refresh();
        execute("select id, qty from t order by id");
        assertEquals(printedTable(response.rows()),
                     "0| NULL\n" +
                     "1| 1\n");
        Asserts.assertSQLError(() -> execute("insert into t(id, qty) values(2, -1)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Failed CONSTRAINT check_1 CHECK (\"qty\" > 0) for values: [2, -1]");
    }

    @Test
    public void test_create_table_with_check_fail_on_update() {
        execute("create table t (id integer primary key, qty integer constraint check_1 check (qty > 0))");
        execute("insert into t(id, qty) values(0, 1)");
        refresh();
        execute("select id, qty from t order by id");
        assertEquals(printedTable(response.rows()), "0| 1\n");
        execute("update t set qty = 1 where id = 0 returning id, qty");
        assertEquals(printedTable(response.rows()), "0| 1\n");
        Asserts.assertSQLError(() -> execute("update t set qty = -1 where id = 0"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5000)
            .hasMessageContaining("Failed CONSTRAINT check_1 CHECK (\"qty\" > 0) for values: [0, -1]");

    }

    @Test
    public void test_alter_table_add_column_succeeds_because_check_constaint_refers_to_self_columns() {
        execute("create table t (id integer primary key, qty integer constraint check_1 check (qty > 0))");
        execute("alter table t add column bazinga integer constraint bazinga_check check(bazinga <> 42)");
        execute("insert into t(id, qty, bazinga) values(0, 1, 100)");
        Asserts.assertSQLError(() -> execute("insert into t(id, qty, bazinga) values(0, 1, 42)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Failed CONSTRAINT bazinga_check CHECK (\"bazinga\" <> 42) for values: [0, 1, 42]");
    }

    @Test
    public void test_alter_table_drop_constraint_removes_the_constraint_and_leaves_other_constraints_in_place() {
        execute("create table t (" +
                      "    id int primary key, " +
                      "    qty int constraint check_qty_gt_zero check(qty > 0), " +
                      "    constraint check_id_ge_zero check (id >= 0)" +
                      ")");
        String selectCheckConstraintsStmt =
            "select table_schema, table_name, constraint_type, constraint_name " +
            "from information_schema.table_constraints " +
            "where table_name='t'" +
            "order by constraint_name";
        execute(selectCheckConstraintsStmt);
        assertThat(response).hasRows(
            "doc| t| CHECK| check_id_ge_zero",
            "doc| t| CHECK| check_qty_gt_zero",
            "doc| t| CHECK| doc_t_id_not_null",
            "doc| t| PRIMARY KEY| t_pk"
        );
        execute("alter table t drop constraint check_id_ge_zero");
        execute(selectCheckConstraintsStmt);
        assertThat(response).hasRows(
            "doc| t| CHECK| check_qty_gt_zero",
            "doc| t| CHECK| doc_t_id_not_null",
            "doc| t| PRIMARY KEY| t_pk"
        );
        execute("insert into t(id, qty) values(-42, 100)");
        Asserts.assertSQLError(() -> execute("insert into t(id, qty) values(0, 0)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Failed CONSTRAINT check_qty_gt_zero CHECK (\"qty\" > 0) for values: [0, 0]");
    }

    @Test
    public void test_can_drop_single_check_constraint() {
        // Dropping a single constraint used to fail before 5.1
        execute("create table t (id int primary key constraint check_id_ge_zero check (id >= 0))");
        execute("alter table t drop constraint check_id_ge_zero");
        String selectCheckConstraintsStmt =
            "select table_schema, table_name, constraint_type, constraint_name " +
                "from information_schema.table_constraints " +
                "where table_name='t'" +
                "order by constraint_name";
        execute(selectCheckConstraintsStmt);
        assertThat(response).hasRows(
            "doc| t| CHECK| doc_t_id_not_null",
            "doc| t| PRIMARY KEY| t_pk"
        );

        execute("insert into t(id) values(-1)");
        refresh();
        execute("select id from t");
        assertThat(response).hasRows("-1");
    }

    @Test
    public void testAlterTable() throws Exception {
        execute("create table test (col1 int) with (number_of_replicas='0-all')");
        ensureYellow();

        execute("select number_of_replicas from information_schema.tables where table_name = 'test'");
        assertEquals("0-all", response.rows()[0][0]);

        execute("alter table test set (number_of_replicas=0)");
        execute("select number_of_replicas from information_schema.tables where table_name = 'test'");
        assertEquals("0", response.rows()[0][0]);
    }

    @Test
    public void testAlterTableAddColumn() {
        execute("create table t (id int primary key) with (number_of_replicas=0)");
        execute("alter table t add column name string");

        execute("select data_type from information_schema.columns where " +
                "table_name = 't' and column_name = 'name'");
        assertThat(response).hasRows("text");

        execute("alter table t add column o object as (age int)");
        execute("select data_type from information_schema.columns where " +
                "table_name = 't' and column_name = 'o'");
        assertThat(response).hasRows("object");
    }

    @Test
    public void testAlterTableAddColumnAsPrimaryKey() throws Exception {
        execute("create table t (id int primary key) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        response = execute("select constraint_name from information_schema.table_constraints " +
            "where table_name = 't' and table_schema = 'doc' order by constraint_name");
        assertThat(response).hasRows(
            "doc_t_id_not_null",
            "t_pk"
        );

        execute("alter table t add column name string primary key");
        response = execute("select constraint_name from information_schema.table_constraints " +
                "where table_name = 't' and table_schema = 'doc' order by constraint_name");

        assertThat(response).hasRows(
            "doc_t_id_not_null",
            "doc_t_name_not_null",
            "t_pk"
        );
    }

    @Test
    public void testAlterTableWithRecordsAddColumnAsPrimaryKey() throws Exception {
        execute("create table t (id int primary key) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        execute("insert into t (id) values(1)");
        refresh();

        Asserts.assertSQLError(() -> execute("alter table t add column name string primary key"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Cannot add a primary key column to a table that isn't empty");
    }

    @Test
    public void testAlterTableWithoutRecordsAddGeneratedColumn() throws Exception {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        execute("alter table t add column id_generated as (id + 1)");
        execute("insert into t (id) values(1)");
        refresh();
        execute("select id, id_generated from t");
        assertThat(response.rows()[0][0]).isEqualTo(1);
        assertThat(response.rows()[0][1]).isEqualTo(2);
    }

    @Test
    public void testAlterTableWithRecordsAddGeneratedColumn() throws Exception {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        execute("insert into t (id) values(1)");
        refresh();

        Asserts.assertSQLError(() -> execute("alter table t add column id_generated as (id + 1)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4004)
            .hasMessageContaining("Cannot add a generated column to a table that isn't empty");
    }

    @Test
    public void testAlterTableAddDotExpression() {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        Asserts.assertSQLError(() -> execute("alter table t add \"o.x\" int"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4008)
            .hasMessageContaining("\"o.x\" contains a dot");
    }

    @Test
    public void testAlterTableAddDotExpressionInSubscript() {
        execute("create table t (id int) clustered into 1 shards with (number_of_replicas=0)");
        Asserts.assertSQLError(() -> execute("alter table t add \"o['x.y']\" int"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4008)
            .hasMessageContaining("\"x.y\" contains a dot");

    }

    @Test
    public void testAlterTableAddObjectColumnToNonExistingObject() {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        execute("alter table t add o['x'] int");
        execute("select column_name from information_schema.columns where " +
                "table_name = 't' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response).hasRowCount(3);

        List<String> fqColumnNames = new ArrayList<>();
        for (Object[] row : response.rows()) {
            fqColumnNames.add((String) row[0]);
        }
        assertThat(fqColumnNames).containsExactly("id", "o", "o['x']");
    }

    @Test
    @UseNewCluster
    public void testAlterTableAddObjectColumnToExistingObject() throws IOException {
        execute("create table t (o object as (x string)) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("alter table t add o['y'] int");

        // Verify that ADD COLUMN gets advanced OID.
        String expectedMapping = "{\"default\":" +
            "{\"dynamic\":\"strict\",\"_meta\":{}," +
            "\"properties\":{\"o\":{\"position\":1,\"oid\":1,\"dynamic\":\"true\"," +
            "\"properties\":" +
            "{\"x\":{\"type\":\"keyword\",\"position\":2,\"oid\":2}," +
            "\"y\":{\"type\":\"integer\",\"position\":3,\"oid\":3}}}}}}";
        assertThat(getIndexMapping("t")).isEqualTo(expectedMapping);

        // column o exists already
        Asserts.assertSQLError(() -> execute("alter table t add o object as (z string)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("The table doc.t already has a column named o");

        execute("select column_name from information_schema.columns where " +
                "table_name = 't' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response).hasRowCount(3L);

        List<String> fqColumnNames = new ArrayList<>();
        for (Object[] row : response.rows()) {
            fqColumnNames.add((String) row[0]);
        }
        assertThat(fqColumnNames).containsExactly("o", "o['x']", "o['y']");


    }

    @Test
    public void testAlterTableAddObjectColumnToExistingObjectNested() throws Exception {
        execute("CREATE TABLE my_table (" +
                "  name string, " +
                "  age integer," +
                "  book object as (isbn string)" +
                ")");
        ensureYellow();
        execute("alter table my_table add column book['author'] object as (\"authorId\" integer)");
        waitNoPendingTasksOnAll();
        execute("select column_name from information_schema.columns where " +
                "table_name = 'my_table' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response).hasRowCount(6);
        assertThat(TestingHelpers.getColumn(response.rows(), 0)).containsExactly(
            "age", "book", "book['author']", "book['author']['authorId']", "book['isbn']", "name");
        execute("alter table my_table add column book['author']['authorName'] string");
        waitNoPendingTasksOnAll();
        execute("select column_name from information_schema.columns where " +
                "table_name = 'my_table' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response).hasRowCount(7);
        assertThat(TestingHelpers.getColumn(response.rows(), 0)).containsExactly(
            "age", "book", "book['author']", "book['author']['authorId']", "book['author']['authorName']", "book['isbn']", "name");

    }

    @Test
    public void testAlterTableAddNestedObjectWithArrayToExistingObject() throws Exception {
        execute("CREATE TABLE my_table (col1 object)");
        ensureYellow();
        execute("ALTER TABLE my_table ADD COLUMN col1['col2'] object as (col3 array(string))");
        waitNoPendingTasksOnAll();
        execute("SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_name = 'my_table' AND table_schema = 'doc' " +
                "ORDER BY column_name asc");
        assertThat(TestingHelpers.getColumn(response.rows(), 0)).containsExactly(
            "col1", "col1['col2']", "col1['col2']['col3']"
        );
        assertThat(TestingHelpers.getColumn(response.rows(), 1)).containsExactly(
            "object", "object", "text_array"
        );

        execute("DROP TABLE my_table");
        ensureYellow();

        execute("CREATE TABLE my_table (col1 object as (col2 object))");
        ensureYellow();
        execute("ALTER TABLE my_table ADD COLUMN col1['col2']['col3'] object as (col4 array(long))");
        waitNoPendingTasksOnAll();
        execute("SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_name = 'my_table' AND table_schema = 'doc' " +
                "ORDER BY column_name asc");
        assertThat(TestingHelpers.getColumn(response.rows(), 0)).containsExactly(
            "col1", "col1['col2']", "col1['col2']['col3']", "col1['col2']['col3']['col4']");
        assertThat(TestingHelpers.getColumn(response.rows(), 1)).containsExactly(
            "object", "object", "object", "bigint_array");
    }

    @Test
    public void testAlterTableAddObjectToObjectArray() throws Exception {
        execute("CREATE TABLE t (" +
                "   attributes ARRAY(" +
                "       OBJECT (STRICT) as (" +
                "           name STRING" +
                "       )" +
                "   )" +
                ")");
        execute("ALTER TABLE t ADD column attributes['is_nice'] BOOLEAN");
        execute("INSERT INTO t (attributes) values ([{name='Trillian', is_nice=True}])");
        refresh();
        execute("select attributes from t");
        assertThat(response).hasRows("[{name=Trillian, is_nice=true}]");
    }

    @Test
    public void testDropTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");

        assertThat(cluster().clusterService().state().metadata().hasIndex("test")).isTrue();

        execute("drop table test");
        assertThat(response).hasRowCount(1);

        assertThat(cluster().clusterService().state().metadata().hasIndex("test")).isFalse();
    }

    @Test
    public void testDropTableIfExistsRaceCondition() throws Exception {
        execute("create table test (name string)");
        execute("drop table test");
        // could fail if the meta data update triggered by the previous drop table wasn't fully propagated in the cluster
        execute("drop table if exists test");
    }

    @Test
    public void testDropUnknownTable() throws Exception {
        Asserts.assertSQLError(() -> execute("drop table test"))
            .hasPGError(UNDEFINED_TABLE)
            .hasHTTPError(NOT_FOUND, 4041)
            .hasMessageContaining("Relation 'test' unknown");
    }

    @Test
    public void testDropTableIfExists() {
        execute("create table test (col1 integer primary key, col2 string)");

        assertThat(cluster().clusterService().state().metadata().hasIndex("test")).isTrue();
        execute("drop table if exists test");
        assertThat(response).hasRowCount(1);
        assertThat(cluster().clusterService().state().metadata().hasIndex("test")).isFalse();
    }

    @Test
    public void testDropIfExistsUnknownTable() throws Exception {
        execute("drop table if exists nonexistent");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testCreateAlterAndDropBlobTable() throws Exception {
        execute("create blob table screenshots with (number_of_replicas=0)");
        execute("alter blob table screenshots set (number_of_replicas=1)");
        execute("select number_of_replicas from information_schema.tables " +
                "where table_schema = 'blob' and table_name = 'screenshots'");
        assertEquals("1", response.rows()[0][0]);
        execute("drop blob table screenshots");
    }

    @Test
    public void testDropIfExistsBlobTable() throws Exception {
        execute("create blob table screenshots with (number_of_replicas=0)");
        execute("drop blob table if exists screenshots");
        assertEquals(response.rowCount(), 1);
    }

    @Test
    public void testDropBlobTableIfExistsUnknownTable() throws Exception {
        execute("drop blob table if exists nonexistent");
        assertThat(response).hasRowCount(0);
    }

    @Test
    public void testAlterShardsOfPartitionedTableAffectsNewPartitions() throws Exception {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   date timestamp with time zone" +
            ") partitioned by(date) " +
            "clustered into 3 shards with (number_of_replicas='0-all')");
        ensureYellow();

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );
        execute("alter table quotes set (number_of_shards=5)");

        String templateName = PartitionName.templateName(Schemas.DOC_SCHEMA_NAME, "quotes");
        GetIndexTemplatesResponse templatesResponse =
            client().admin().indices().getTemplates(new GetIndexTemplatesRequest(templateName)).get();
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).settings();
        assertThat(templateSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0)).isEqualTo(5);

        execute("insert into quotes (id, quote, date) values (?, ?, ?)",
            new Object[]{3, "Time is a illusion. Lunchtime doubles so", 1495961200000L}
        );
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "quotes"), Arrays.asList("1495961200000"));

        execute("select number_of_shards from information_schema.table_partitions where partition_ident = ? and table_name = ?",
            $(partitionName.ident(), partitionName.relationName().name()));
        assertThat(response).hasRows(new Object[] { 5 });
    }

    @Test
    public void testAlterShardsTableCombinedWithOtherSettingsIsInvalid() {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   date timestamp with time zone" +
            ") clustered into 3 shards with (number_of_replicas='0-all')");

        Asserts.assertSQLError(() -> execute("alter table quotes set (number_of_shards=1, number_of_replicas='1-all')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Setting [number_of_shards] cannot be combined with other settings");
    }

    @Test
    public void testAlterShardsPartitionCombinedWithOtherSettingsIsInvalid() {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   date timestamp with time zone" +
            ") partitioned by(date) " +
            "clustered into 3 shards with (number_of_replicas='0-all')");

        execute("insert into quotes (id, quote, date) values (?, ?, ?), (?, ?, ?)",
            new Object[]{
                1, "Don't panic", 1395874800000L,
                2, "Now panic", 1395961200000L}
        );

        Asserts.assertSQLError(() -> execute("alter table quotes partition (date=1395874800000) " +
                                   "set (number_of_shards=1, number_of_replicas='1-all')"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Setting [number_of_shards] cannot be combined with other settings");
    }

    @Test
    public void testCreateTableWithCustomSchema() throws Exception {
        execute("create table a.t (name string) with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into a.t (name) values ('Ford')");
        assertThat(response).hasRowCount(1);
        refresh();

        execute("select name from a.t");
        assertThat(response).hasRowCount(1);
        assertThat(response).hasRows("Ford");

        execute("select table_schema from information_schema.tables where table_name = 't'");
        assertThat(response).hasRowCount(1);
        assertThat(response).hasRows("a");
    }

    @Test
    public void testCreateTableWithIllegalCustomSchemaCheckedByES() throws Exception {
        Asserts.assertSQLError(() -> execute("create table \"AA A\".t (name string) with (number_of_replicas=0)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4002)
            .hasMessageContaining("Relation name \"AA A.t\" is invalid.");

    }

    @Test
    public void testDropTableWithCustomSchema() throws Exception {
        execute("create table a.t (name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("drop table a.t");
        assertThat(response).hasRowCount(1);

        execute("select table_schema from information_schema.tables where table_name = 't'");
        assertThat(response).hasRowCount(0);
    }

    @Test
    @UseNewCluster
    public void testCreateTableWithGeneratedColumn() throws Exception {
        execute(
            "create table test (" +
            "   ts timestamp with time zone," +
            "   day as date_trunc('day', ts)) with (number_of_replicas=0)");
        ensureYellow();
        String expectedMapping = "{\"default\":" +
                                 "{\"dynamic\":\"strict\"," +
                                 "\"_meta\":{" +
                                 "\"generated_columns\":{\"day\":\"date_trunc('day', ts)\"}}," +
                                 "\"properties\":{" +
                                 "\"ts\":{\"type\":\"date\",\"position\":1,\"oid\":1,\"format\":\"epoch_millis||strict_date_optional_time\"}," +
                                 "\"day\":{\"type\":\"date\",\"position\":2,\"oid\":2,\"format\":\"epoch_millis||strict_date_optional_time\"}" +
                                 "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
    }

    @Test
    @UseNewCluster
    public void testAddGeneratedColumnToTableWithExistingGeneratedColumns() throws Exception {
        execute(
            "create table test (" +
            "   ts timestamp with time zone," +
            "   day as date_trunc('day', ts)) with (number_of_replicas=0)");
        execute("alter table test add column added timestamp with time zone generated always as date_trunc('day', ts)");
        String expectedMapping = "{\"default\":" +
                                 "{\"dynamic\":\"strict\"," +
                                 "\"_meta\":{" +
                                 "\"generated_columns\":{" +
                                 "\"day\":\"date_trunc('day', ts)\"," +
                                 "\"added\":\"date_trunc('day', ts)\"}}," +
                                 "\"properties\":{" +
                                 "\"ts\":{\"type\":\"date\",\"position\":1,\"oid\":1,\"format\":\"epoch_millis||strict_date_optional_time\"}," +
                                 "\"day\":{\"type\":\"date\",\"position\":2,\"oid\":2,\"format\":\"epoch_millis||strict_date_optional_time\"}," +
                                 "\"added\":{\"type\":\"date\",\"position\":3,\"oid\":3,\"format\":\"epoch_millis||strict_date_optional_time\"}" +
                                 "}}}";

        JSONAssert.assertEquals(getIndexMapping("test"), expectedMapping, JSONCompareMode.LENIENT);
    }


    @Test
    public void test_alter_table_cannot_add_broken_generated_column() throws Exception {
        execute("create table tbl (x int)");

        Asserts.assertSQLError(
            () -> execute("alter table tbl add column ts timestamp without time zone generated always as 'foobar'"))
            .hasMessageContaining("Cannot cast `'foobar'` of type `text` to type `timestamp without time zone`")
            .hasPGError(PGErrorStatus.INTERNAL_ERROR)
            .hasHTTPError(HttpResponseStatus.BAD_REQUEST, 4000);
    }

    @Test
    public void test_alter_table_add_column_keeps_existing_meta_information() throws Exception {
        execute(
            """
                     CREATE TABLE tbl (
                        author TEXT NOT NULL,
                        INDEX author_ft USING FULLTEXT (author) WITH (analyzer = 'standard')
                    )
                """);

        execute("ALTER TABLE tbl ADD COLUMN dummy text NOT NULL");

        execute("show create table tbl");
        assertThat((String) response.rows()[0][0]).startsWith(
            """
                CREATE TABLE IF NOT EXISTS "doc"."tbl" (
                   "author" TEXT NOT NULL,
                   "dummy" TEXT NOT NULL,
                   INDEX "author_ft" USING FULLTEXT ("author") WITH (
                      analyzer = 'standard'
                   )
                )
                """.stripIndent()
        );
    }

    @Test
    public void test_geo_shape_can_be_not_null() {
        execute("create table t (col geo_shape INDEX using QUADTREE with (precision='1m', distance_error_pct='0.25') NOT NULL)");
        execute("show create table t");
        assertThat((String) response.rows()[0][0]).startsWith(
            """
                CREATE TABLE IF NOT EXISTS "doc"."t" (
                   "col" GEO_SHAPE NOT NULL INDEX USING QUADTREE WITH (
                      distance_error_pct = 0.25,
                      precision = '1.0m'
                   )
                )
                """.stripIndent()
        );
    }

    @Test
    public void test_add_column_all_supported_configs_applied_to_altered_table() throws Exception {
        // add at least one of each constraints (not null, PK, check, generated) to the initial table
        // to make sure that merge logic doesn't override existing mapping
        execute("CREATE TABLE tbl (id int primary key constraint id_check check (id>0), gen_col int generated always as 123 not null)");

        execute("ALTER TABLE tbl ADD COLUMN col1 text " +
            "generated always as 'test' " +
            "not null " +
            "constraint test_check check (col1!='d') " +
            "INDEX USING FULLTEXT WITH (analyzer = 'simple')");

        execute("show create table tbl");
        assertThat((String) response.rows()[0][0]).startsWith(
            """
                CREATE TABLE IF NOT EXISTS "doc"."tbl" (
                   "id" INTEGER NOT NULL,
                   "gen_col" INTEGER GENERATED ALWAYS AS 123 NOT NULL,
                   "col1" TEXT GENERATED ALWAYS AS 'test' NOT NULL INDEX USING FULLTEXT WITH (
                      analyzer = 'simple'
                   ),
                   PRIMARY KEY ("id"),
                   CONSTRAINT id_check CHECK("id" > 0),
                   CONSTRAINT test_check CHECK("col1" <> 'd')
                )""".stripIndent()
        );

        // test other options, which couldn't be tested in the first scenario:
        // Primary key can be used here as we don't have not null
        // and doc values flag can be disabled on not fulltext columns (otherwise it's ignored)
        execute("ALTER TABLE tbl ADD COLUMN col2 varchar(100) " +
            "primary key " +
            "storage with (columnstore=false)"
        );
        execute("show create table tbl");
        assertThat((String) response.rows()[0][0]).startsWith(
            """
                CREATE TABLE IF NOT EXISTS "doc"."tbl" (
                   "id" INTEGER NOT NULL,
                   "gen_col" INTEGER GENERATED ALWAYS AS 123 NOT NULL,
                   "col1" TEXT GENERATED ALWAYS AS 'test' NOT NULL INDEX USING FULLTEXT WITH (
                      analyzer = 'simple'
                   ),
                   "col2" VARCHAR(100) NOT NULL STORAGE WITH (
                      columnstore = false
                   ),
                   PRIMARY KEY ("id", "col2"),
                   CONSTRAINT id_check CHECK("id" > 0),
                   CONSTRAINT test_check CHECK("col1" <> 'd')
                )""".stripIndent()
        );
    }

    @Test
    public void test_add_geo_shape_column() throws Exception {
        execute("CREATE TABLE tbl (id INTEGER)");

        execute("ALTER TABLE tbl ADD COLUMN g geo_shape " +
            "GENERATED ALWAYS AS 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))' " +
            "INDEX using QUADTREE with (precision='123m', distance_error_pct='0.123')"
        );

        execute("show create table tbl");
        assertThat((String) response.rows()[0][0]).startsWith(
            """
                CREATE TABLE IF NOT EXISTS "doc"."tbl" (
                   "id" INTEGER,
                   "g" GEO_SHAPE GENERATED ALWAYS AS 'POLYGON (( 5 5, 30 5, 30 30, 5 30, 5 5 ))' INDEX USING QUADTREE WITH (
                      distance_error_pct = 0.123,
                      precision = '123.0m'
                   )
                )""".stripIndent()
        );
    }

    @Test
    public void test_add_column_not_null_constraint_added_to_a_nested_column() {
        Map<String, Object> ab = Map.of("a", Map.of("b", "dummy"));

        // non-existing object
        execute("create table t (o object)");
        execute("alter table t add column o2['a']['b'] text not null");
        Asserts.assertSQLError(() -> execute("insert into t (o2) values ({\"a\" = {\"b\" = null}})"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("\"o2['a']['b']\" must not be null");

        // existing object
        execute("alter table t add column o['a']['b'] text not null");
        Asserts.assertSQLError(() -> execute("insert into t (o2, o) values (?, {\"a\" = {\"b\" = null}})", new Object[] { ab }))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("\"o['a']['b']\" must not be null");

        // object with multiple children
        execute("alter table t add column o3 object as (a object as (b text not null), c int not null)");
        // one leaf of o3
        Asserts.assertSQLError(() -> execute(
                "insert into t (o, o2) values ({\"a\" = {\"b\" = 'test'}}, {\"a\" = {\"b\" = 'test'}})"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("\"o3['a']['b']\" must not be null");
        // another leaf of o3
        Asserts.assertSQLError(() -> execute(
                "insert into t (o, o2, o3) values ({\"a\" = {\"b\" = 'test'}}, {\"a\" = {\"b\" = 'test'}}, {\"a\" = {\"b\" = 'test'}, \"c\" = null})"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("\"o3['c']\" must not be null");
    }


    @Test
    public void test_add_multiple_columns_with_constraints_in_sub_columns() {
        execute("create table t(id integer primary key)");
        /*
         Adding multiple columns:
         - 2 columns sharing sub-path to verify that we don't add multiple times common path part to AddColumnRequest
         - object with branching (with multiple leaves)
         - some primitive columns with different constraints
         - multiple primary keys columns, having some none-primary columns in-between
         With dynamic mapping updates we can add many columns but they don't have constraints so
         all added columns have different constraints as it's the only use-case hitting related code path.
         */
        execute("""
            alter table t
                add column o1['a1']['b1'] text generated always as 'val1' ,
                add column o1['a1']['c1'] int constraint leaf_check check (o1['a1']['c1'] > 10),
                add column o2 object as (a2 object as (b2 text not null), c2 int),
                add column int_col INTEGER constraint int_check check (int_col > 20) primary key,
                add column long_col LONG generated always as 30,
                add column analyzed_col TEXT INDEX USING FULLTEXT WITH (analyzer = 'simple')
            """
        );

        execute("show create table t");
        assertThat((String) response.rows()[0][0]).startsWith(
            """
                CREATE TABLE IF NOT EXISTS "doc"."t" (
                   "id" INTEGER NOT NULL,
                   "o1" OBJECT(DYNAMIC) AS (
                      "a1" OBJECT(DYNAMIC) AS (
                         "b1" TEXT GENERATED ALWAYS AS 'val1',
                         "c1" INTEGER
                      )
                   ),
                   "o2" OBJECT(DYNAMIC) AS (
                      "a2" OBJECT(DYNAMIC) AS (
                         "b2" TEXT NOT NULL
                      ),
                      "c2" INTEGER
                   ),
                   "int_col" INTEGER NOT NULL,
                   "long_col" BIGINT GENERATED ALWAYS AS 30,
                   "analyzed_col" TEXT INDEX USING FULLTEXT WITH (
                      analyzer = 'simple'
                   ),
                   PRIMARY KEY ("id", "int_col"),
                   CONSTRAINT leaf_check CHECK("o1"['a1']['c1'] > 10),
                   CONSTRAINT int_check CHECK("int_col" > 20)
                )""".stripIndent()
        );

        // We test explicitly only CHECK since not-null and generated is part of a Reference and this is same behavior as adding single column.
        // CHECK constraints are streamed separately and adding multiple columns is the only use case of adding multiple checks at once.
        Asserts.assertSQLError(
            () -> execute("insert into t (id, o2, int_col) values (1, {\"a2\" = {\"b2\" = 'test'}}, 19)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Failed CONSTRAINT int_check CHECK (\"int_col\" > 20)");

        Asserts.assertSQLError(
            () -> execute("insert into t (id, o2, o1, int_col) values (1, {\"a2\" = {\"b2\" = 'test'}}, {\"a1\" = {\"c1\" = 9}}, 25)"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("Failed CONSTRAINT leaf_check CHECK (\"o1\"['a1']['c1'] > 10)");
    }
}
