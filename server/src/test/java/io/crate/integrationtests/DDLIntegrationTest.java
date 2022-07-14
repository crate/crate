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
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.rtsp.RtspResponseStatuses.INTERNAL_SERVER_ERROR;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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
import org.hamcrest.Matchers;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.testing.Asserts;
import io.crate.testing.SQLErrorMatcher;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseRandomizedSchema;
import io.netty.handler.codec.http.HttpResponseStatus;

@IntegTestCase.ClusterScope()
@UseRandomizedSchema(random = false)
public class DDLIntegrationTest extends IntegTestCase {

    @Test
    public void testCreateTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) " +
                "clustered into 5 shards with (number_of_replicas = 1, \"write.wait_for_active_shards\"=1)");
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{" +
                                 "\"primary_keys\":[\"col1\"]}," +
                                 "\"properties\":{" +
                                 // doc_values: true is default and not included
                                 "\"col1\":{\"type\":\"integer\",\"position\":1}," +
                                 "\"col2\":{\"type\":\"keyword\",\"position\":2}" +
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
        ensureYellow();
        assertThrowsMatches(() -> execute("create table test (col1 integer primary key, col2 string)"),
                     isSQLError(is("Relation 'doc.test' already exists."),
                                DUPLICATE_TABLE,
                                CONFLICT,
                                4093)
        );
    }

    @Test
    public void testCreateTableWithReplicasAndShards() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)" +
                "clustered by (col1) into 10 shards with (number_of_replicas=2, \"write.wait_for_active_shards\"=1)");
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\"," +
                                 "\"_meta\":{" +
                                 "\"primary_keys\":[\"col1\"]," +
                                 "\"routing\":\"col1\"}," +
                                 "\"properties\":{" +
                                 "\"col1\":{\"type\":\"integer\"}," +
                                 "\"col2\":{\"type\":\"keyword\"}" +
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
    public void testCreateTableWithStrictColumnPolicy() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) " +
                "clustered into 5 shards " +
                "with (column_policy='strict', number_of_replicas = 0)");
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{" +
                                 "\"primary_keys\":[\"col1\"]}," +
                                 "\"properties\":{" +
                                 "\"col1\":{\"type\":\"integer\",\"position\":1}," +
                                 "\"col2\":{\"type\":\"keyword\",\"position\":2}" +
                                 "}}}";

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
    public void testCreateGeoShapeExplicitIndex() throws Exception {
        execute("create table test (col1 geo_shape INDEX using QUADTREE with (precision='1m', distance_error_pct='0.25'))");
        ensureYellow();
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{}," +
                                 "\"properties\":{" +
                                 "\"col1\":{\"type\":\"geo_shape\",\"tree\":\"quadtree\",\"position\":1,\"precision\":\"1.0m\",\"distance_error_pct\":0.25}}}}";
        assertEquals(expectedMapping, getIndexMapping("test"));
    }

    @Test
    public void testCreateColumnWithDefaultExpression() throws Exception {
        execute("create table test (col1 text default 'foo')");
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{}," +
                                 "\"properties\":{\"col1\":{\"type\":\"keyword\",\"position\":1,\"default_expr\":\"'foo'\"}}}}";
        assertEquals(expectedMapping, getIndexMapping("test"));

    }

    @Test
    public void testCreateGeoShape() throws Exception {
        execute("create table test (col1 geo_shape)");
        ensureYellow();
        String expectedMapping = "{\"default\":{" +
                                 "\"dynamic\":\"strict\",\"_meta\":{}," +
                                 "\"properties\":{\"col1\":{\"type\":\"geo_shape\",\"position\":1}}}}";
        assertEquals(expectedMapping, getIndexMapping("test"));

    }

    @Test
    public void testGeoShapeInvalidPrecision() throws Exception {
        assertThrowsMatches(() -> execute("create table test (col1 geo_shape INDEX using QUADTREE with (precision='10%'))"),
                     isSQLError(is("Value '10%' of setting precision is not a valid distance unit"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000)
        );
    }

    @Test
    public void testGeoShapeInvalidDistance() throws Exception {
        assertThrowsMatches(() -> execute(
            "create table test (col1 geo_shape INDEX using QUADTREE with (distance_error_pct=true))"),
                     isSQLError(is("Value 'true' of setting distance_error_pct is not a float value"),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4000)
        );
    }

    @Test
    public void testUnknownGeoShapeSetting() throws Exception {
        assertThrowsMatches(() -> execute("create table test (col1 geo_shape INDEX using QUADTREE with (does_not_exist=false))"),
                     isSQLError(is("Setting \"does_not_exist\" ist not supported on geo_shape index"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000)
        );
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

        assertThrowsMatches(
            () -> execute("select quote from quotes where quote = ?", new Object[]{quote}),
            isSQLError(
                containsString("Cannot search on field [quote] since it is not indexed."),
                INTERNAL_ERROR,
                BAD_REQUEST,
                4000
            )
        );
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
    public void testCreateTableWithCompositeIndex() throws Exception {
        execute("create table novels (title string, description string, " +
                "index title_desc_fulltext using fulltext(title, description) " +
                "with(analyzer='stop')) with (number_of_replicas = 0)");

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
        assertThrowsMatches(() -> execute("insert into t(id, qty) values(2, -1)"),
                     isSQLError(containsString("Failed CONSTRAINT check_1 CHECK (\"qty\" > 0) and values"),
                     INTERNAL_ERROR,
                     BAD_REQUEST,
                     4000));
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
        assertThrowsMatches(() -> execute("update t set qty = -1 where id = 0"),
                     isSQLError(containsString("Failed CONSTRAINT check_1 CHECK (\"qty\" > 0) and values"),
                                INTERNAL_ERROR,
                                INTERNAL_SERVER_ERROR,
                                5000));

    }

    @Test
    public void test_alter_table_add_column_succedds_because_check_constaint_refers_to_self_columns() {
        execute("create table t (id integer primary key, qty integer constraint check_1 check (qty > 0))");
        execute("alter table t add column bazinga integer constraint bazinga_check check(bazinga <> 42)");
        execute("insert into t(id, qty, bazinga) values(0, 1, 100)");
        assertThrowsMatches(() -> execute("insert into t(id, qty, bazinga) values(0, 1, 42)"),
                     isSQLError(containsString("Failed CONSTRAINT bazinga_check CHECK (\"bazinga\" <> 42) and values {qty=1, id=0, bazinga=42}"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));
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
        assertThat(printedTable(response.rows()), is(
            "doc| t| CHECK| check_id_ge_zero\n" +
            "doc| t| CHECK| check_qty_gt_zero\n" +
            "doc| t| CHECK| doc_t_id_not_null\n" +
            "doc| t| PRIMARY KEY| t_pk\n"
        ));
        execute("alter table t drop constraint check_id_ge_zero");
        execute(selectCheckConstraintsStmt);
        assertThat(printedTable(response.rows()), is(
            "doc| t| CHECK| check_qty_gt_zero\n" +
            "doc| t| CHECK| doc_t_id_not_null\n" +
            "doc| t| PRIMARY KEY| t_pk\n"
        ));
        execute("insert into t(id, qty) values(-42, 100)");
        assertThrowsMatches(() -> execute("insert into t(id, qty) values(0, 0)"),
                     isSQLError(is("Failed CONSTRAINT check_qty_gt_zero CHECK (\"qty\" > 0) and values {qty=0, id=0}"),
                         INTERNAL_ERROR,
                         BAD_REQUEST,
                         4000));
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
        assertThat(response.rows()[0][0], is("text"));

        execute("alter table t add column o object as (age int)");
        execute("select data_type from information_schema.columns where " +
                "table_name = 't' and column_name = 'o'");
        assertThat((String) response.rows()[0][0], is("object"));
    }

    @Test
    public void testAlterTableAddColumnAsPrimaryKey() throws Exception {
        execute("create table t (id int primary key) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        response = execute("select constraint_name from information_schema.table_constraints " +
            "where table_name = 't' and table_schema = 'doc' order by constraint_name");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows()[0][0], is("doc_t_id_not_null"));
        assertThat(response.rows()[1][0], is("t_pk"));

        execute("alter table t add column name string primary key");
        response = execute("select constraint_name from information_schema.table_constraints " +
                "where table_name = 't' and table_schema = 'doc' order by constraint_name");

        assertThat(response.rowCount(), is(3L));
        assertThat(response.rows()[0][0], is("doc_t_id_not_null"));
        assertThat(response.rows()[1][0], is("doc_t_name_not_null"));
        assertThat(response.rows()[2][0], is("t_pk"));

    }

    @Test
    public void testAlterTableWithRecordsAddColumnAsPrimaryKey() throws Exception {
        execute("create table t (id int primary key) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (id) values(1)");
        refresh();

        assertThrowsMatches(() -> execute("alter table t add column name string primary key"),
                            isSQLError(is("Cannot add a primary key column to a table that isn't empty"),
                                       INTERNAL_ERROR,
                                       BAD_REQUEST,
                                       4004));
    }

    @Test
    public void testAlterTableWithoutRecordsAddGeneratedColumn() throws Exception {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("alter table t add column id_generated as (id + 1)");
        execute("insert into t (id) values(1)");
        refresh();
        execute("select id, id_generated from t");
        assertThat(response.rows()[0][0], is(1));
        assertThat(response.rows()[0][1], is(2));
    }

    @Test
    public void testAlterTableWithRecordsAddGeneratedColumn() throws Exception {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("insert into t (id) values(1)");
        refresh();

        assertThrowsMatches(() -> execute("alter table t add column id_generated as (id + 1)"),
                     isSQLError(is("Cannot add a generated column to a table that isn't empty"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4004));
    }

    @Test
    public void testAlterTableAddDotExpression() {
        execute("create table t (id int) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        assertThrowsMatches(() -> execute("alter table t add \"o.x\" int"),
                     isSQLError(is("\"o.x\" contains a dot"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4008));
    }

    @Test
    public void testAlterTableAddDotExpressionInSubscript() {
        execute("create table t (id int) clustered into 1 shards with (number_of_replicas=0)");
        assertThrowsMatches(() -> execute("alter table t add \"o['x.y']\" int"),
                            isSQLError(is("\"x.y\" contains a dot"),
                                       INTERNAL_ERROR,
                                       BAD_REQUEST,
                                       4008));

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
        assertThat(response.rowCount(), is(3L));

        List<String> fqColumnNames = new ArrayList<>();
        for (Object[] row : response.rows()) {
            fqColumnNames.add((String) row[0]);
        }
        assertThat(fqColumnNames, Matchers.contains("id", "o", "o['x']"));
    }

    @Test
    public void testAlterTableAddObjectColumnToExistingObject() {
        execute("create table t (o object as (x string)) " +
                "clustered into 1 shards " +
                "with (number_of_replicas=0)");
        ensureYellow();
        execute("alter table t add o['y'] int");
        // column o exists already
        assertThrowsMatches(() -> execute("alter table t add o object as (z string)"),
                     isSQLError(containsString("The table doc.t already has a column named o"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));

        execute("select column_name from information_schema.columns where " +
                "table_name = 't' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response.rowCount(), is(3L));

        List<String> fqColumnNames = new ArrayList<>();
        for (Object[] row : response.rows()) {
            fqColumnNames.add((String) row[0]);
        }
        assertThat(fqColumnNames, Matchers.contains("o", "o['x']", "o['y']"));
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
        assertThat(response.rowCount(), is(6L));
        assertThat(TestingHelpers.getColumn(response.rows(), 0),
            is(Matchers.<Object>arrayContaining("age", "book", "book['author']", "book['author']['authorId']", "book['isbn']", "name")));
        execute("alter table my_table add column book['author']['authorName'] string");
        waitNoPendingTasksOnAll();
        execute("select column_name from information_schema.columns where " +
                "table_name = 'my_table' and table_schema='doc'" +
                "order by column_name asc");
        assertThat(response.rowCount(), is(7L));
        assertThat(TestingHelpers.getColumn(response.rows(), 0),
            is(Matchers.<Object>arrayContaining("age", "book", "book['author']", "book['author']['authorId']", "book['author']['authorName']", "book['isbn']", "name")));

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
        assertThat(TestingHelpers.getColumn(response.rows(), 0),
            is(Matchers.<Object>arrayContaining("col1", "col1['col2']", "col1['col2']['col3']")));
        assertThat(TestingHelpers.getColumn(response.rows(), 1),
            is(Matchers.arrayContaining("object", "object", "text_array")));

        execute("DROP TABLE my_table");
        ensureYellow();

        execute("CREATE TABLE my_table (col1 object as (col2 object))");
        ensureYellow();
        execute("ALTER TABLE my_table ADD COLUMN col1['col2']['col3'] object as (col4 array(long))");
        waitNoPendingTasksOnAll();
        execute("SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_name = 'my_table' AND table_schema = 'doc' " +
                "ORDER BY column_name asc");
        assertThat(TestingHelpers.getColumn(response.rows(), 0),
            is(Matchers.<Object>arrayContaining("col1", "col1['col2']", "col1['col2']['col3']", "col1['col2']['col3']['col4']")));
        assertThat(TestingHelpers.getColumn(response.rows(), 1),
            is(Matchers.arrayContaining("object", "object", "object", "bigint_array")));
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
        ensureYellow();
        execute("ALTER TABLE t ADD column attributes['is_nice'] BOOLEAN");
        execute("INSERT INTO t (attributes) values ([{name='Trillian', is_nice=True}])");
        refresh();
        execute("select attributes from t");
        assertThat(((List<Object>)response.rows()[0][0]).get(0), is(Map.of("name", "Trillian", "is_nice", true)));
    }

    @Test
    public void testDropTable() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");

        assertThat(internalCluster().clusterService().state().metadata().hasIndex("test"), is(true));

        execute("drop table test");
        assertThat(response.rowCount(), is(1L));

        assertThat(internalCluster().clusterService().state().metadata().hasIndex("test"), is(false));
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
        assertThrowsMatches(() -> execute("drop table test"),
                     isSQLError(is("Relation 'test' unknown"), UNDEFINED_TABLE, NOT_FOUND, 4041));
    }

    @Test
    public void testDropTableIfExists() {
        execute("create table test (col1 integer primary key, col2 string)");

        assertThat(internalCluster().clusterService().state().metadata().hasIndex("test"), is(true));
        execute("drop table if exists test");
        assertThat(response.rowCount(), is(1L));
        assertThat(internalCluster().clusterService().state().metadata().hasIndex("test"), is(false));
    }

    @Test
    public void testDropIfExistsUnknownTable() throws Exception {
        execute("drop table if exists nonexistent");
        assertThat(response.rowCount(), is(0L));
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
        assertThat(response.rowCount(), is(0L));
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
        Settings templateSettings = templatesResponse.getIndexTemplates().get(0).getSettings();
        assertThat(templateSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 0), is(5));

        execute("insert into quotes (id, quote, date) values (?, ?, ?)",
            new Object[]{3, "Time is a illusion. Lunchtime doubles so", 1495961200000L}
        );
        PartitionName partitionName = new PartitionName(
            new RelationName("doc", "quotes"), Arrays.asList("1495961200000"));

        execute("select number_of_shards from information_schema.table_partitions where partition_ident = ? and table_name = ?",
            $(partitionName.ident(), partitionName.relationName().name()));
        assertThat(response.rows()[0][0], is(5));
    }

    @Test
    public void testAlterShardsTableCombinedWithOtherSettingsIsInvalid() {
        execute(
            "create table quotes (" +
            "   id integer," +
            "   quote string," +
            "   date timestamp with time zone" +
            ") clustered into 3 shards with (number_of_replicas='0-all')");

        assertThrowsMatches(() -> execute("alter table quotes set (number_of_shards=1, number_of_replicas='1-all')"),
                     isSQLError(is("Setting [number_of_shards] cannot be combined with other settings"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));
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

        assertThrowsMatches(() -> execute("alter table quotes partition (date=1395874800000) " +
                                   "set (number_of_shards=1, number_of_replicas='1-all')"),
                     isSQLError(is("Setting [number_of_shards] cannot be combined with other settings"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));
    }

    @Test
    public void testCreateTableWithCustomSchema() throws Exception {
        execute("create table a.t (name string) with (number_of_replicas=0)");
        ensureYellow();

        execute("insert into a.t (name) values ('Ford')");
        assertThat(response.rowCount(), is(1L));
        refresh();

        execute("select name from a.t");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("Ford"));

        execute("select table_schema from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), is(1L));
        assertThat((String) response.rows()[0][0], is("a"));
    }

    @Test
    public void testCreateTableWithIllegalCustomSchemaCheckedByES() throws Exception {
        assertThrowsMatches(() -> execute("create table \"AAA\".t (name string) with (number_of_replicas=0)"),
                     isSQLError(is("Relation name \"AAA.t\" is invalid."), INTERNAL_ERROR, BAD_REQUEST, 4002));

    }

    @Test
    public void testDropTableWithCustomSchema() throws Exception {
        execute("create table a.t (name string) with (number_of_replicas=0)");
        ensureYellow();
        execute("drop table a.t");
        assertThat(response.rowCount(), is(1L));

        execute("select table_schema from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
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
                                 "\"day\":{\"type\":\"date\",\"position\":2,\"format\":\"epoch_millis||strict_date_optional_time\"}," +
                                 "\"ts\":{\"type\":\"date\",\"position\":1,\"format\":\"epoch_millis||strict_date_optional_time\"}" +
                                 "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
    }

    @Test
    public void testAddGeneratedColumnToTableWithExistingGeneratedColumns() throws Exception {
        execute(
            "create table test (" +
            "   ts timestamp with time zone," +
            "   day as date_trunc('day', ts)) with (number_of_replicas=0)");
        execute("alter table test add column added timestamp with time zone generated always as date_trunc('day', ts)");
        ensureYellow();
        String expectedMapping = "{\"default\":" +
                                 "{\"dynamic\":\"strict\"," +
                                 "\"_meta\":{" +
                                 "\"generated_columns\":{" +
                                 "\"added\":\"date_trunc('day', ts)\"," +
                                 "\"day\":\"date_trunc('day', ts)\"}}," +
                                 "\"properties\":{" +
                                 "\"added\":{\"type\":\"date\",\"position\":3,\"format\":\"epoch_millis||strict_date_optional_time\"}," +
                                 "\"day\":{\"type\":\"date\",\"position\":2,\"format\":\"epoch_millis||strict_date_optional_time\"}," +
                                 "\"ts\":{\"type\":\"date\",\"position\":1,\"format\":\"epoch_millis||strict_date_optional_time\"}" +
                                 "}}}";

        assertEquals(expectedMapping, getIndexMapping("test"));
    }


    @Test
    public void test_alter_table_cannot_add_broken_generated_column() throws Exception {
        execute("create table tbl (x int)");

        Asserts.assertThrowsMatches(
            () -> execute("alter table tbl add column ts timestamp without time zone generated always as 'foobar'"),
            SQLErrorMatcher.isSQLError(
                is("Cannot cast `'foobar'` of type `text` to type `timestamp without time zone`"),
                PGErrorStatus.INTERNAL_ERROR,
                HttpResponseStatus.BAD_REQUEST,
                4000
            )
        );
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
        assertThat((String) response.rows()[0][0], startsWith(
            """
                CREATE TABLE IF NOT EXISTS "doc"."tbl" (
                   "author" TEXT NOT NULL,
                   "dummy" TEXT NOT NULL,
                   INDEX "author_ft" USING FULLTEXT ("author") WITH (
                      analyzer = 'standard'
                   )
                )
                """.stripIndent()
        ));
    }
}
