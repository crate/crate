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

import io.crate.metadata.IndexMappings;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseRandomizedSchema;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrowsMatches;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.TestingHelpers.printedTable;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class InformationSchemaTest extends SQLIntegrationTestCase {

    @After
    public void dropLeftoverViews() {
        execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'VIEW'");
        for (Object[] row : response.rows()) {
            logger.info("DROP VIEW {}.{}", row[0], row[1]);
            execute("DROP VIEW " + String.format("\"%s\".\"%s\"", row[0], row[1]));
        }
    }

    @Test
    public void testDefaultTables() {
        execute("select * from information_schema.tables order by table_schema, table_name");
        assertEquals(49L, response.rowCount());

        assertThat(printedTable(response.rows()), is(
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| character_sets| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| columns| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| key_column_usage| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| referential_constraints| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| routines| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| schemata| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| sql_features| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| table_constraints| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| table_partitions| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| tables| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| views| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_am| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_attrdef| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_attribute| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_class| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_constraint| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_database| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_description| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_enum| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_index| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_indexes| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_namespace| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_proc| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_publication| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_range| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_roles| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_settings| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_stats| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_tablespace| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_type| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| allocations| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| checks| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| cluster| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| health| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| jobs| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| jobs_log| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| jobs_metrics| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| node_checks| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| nodes| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| operations| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| operations_log| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| privileges| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| repositories| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| segments| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| shards| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| snapshot_restore| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| snapshots| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| summits| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| users| sys| BASE TABLE| NULL\n"
            )
        );
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testSelectViewsFromInformationSchema() {
        execute("CREATE TABLE t1 (id INTEGER, name STRING) CLUSTERED INTO 2 SHARDS WITH (number_of_replicas=1)");
        execute("CREATE VIEW t1_view1 AS SELECT * FROM t1 WHERE name = 'foo'");
        execute("CREATE VIEW t1_view2 AS SELECT id FROM t1 WHERE name = 'foo'");

        // SELECT information_schema.tables
        execute("SELECT table_type, table_name, number_of_shards, number_of_replicas " +
                "FROM information_schema.tables " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        assertThat(printedTable(response.rows()), is("BASE TABLE| t1| 2| 1\n" +
                                                                    "VIEW| t1_view1| NULL| NULL\n" +
                                                                    "VIEW| t1_view2| NULL| NULL\n"));

        // SELECT information_schema.views
        execute("SELECT table_name, view_definition " +
                "FROM information_schema.views " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        Object[][] rows = response.rows();
        assertThat(rows[0][0], is("t1_view1"));
        assertThat(rows[0][1],
            is("SELECT *\n" +
               "FROM \"t1\"\n" +
               "WHERE \"name\" = 'foo'\n"));
        assertThat(rows[1][0], is("t1_view2"));
        assertThat(rows[1][1],
            is("SELECT \"id\"\n" +
               "FROM \"t1\"\n" +
               "WHERE \"name\" = 'foo'\n"));

        // SELECT information_schema.columns
        execute("SELECT table_name, column_name " +
                "FROM information_schema.columns " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        assertThat(printedTable(response.rows()), is("t1| id\n" +
                                                                    "t1| name\n" +
                                                                    "t1_view1| id\n" +
                                                                    "t1_view1| name\n" +
                                                                    "t1_view2| id\n"));

        // After dropping the target table of the view, the view still shows up in information_schema.tables and
        // information_schema.views,  but not in information_schema.columns, because the SELECT statement could not be
        // analyzed.
        execute("DROP TABLE t1");

        execute("SELECT table_name, view_definition " +
                "FROM information_schema.views " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        assertThat(rows[0][0], is("t1_view1"));
        assertThat(rows[0][1],
            is("SELECT *\n" +
               "FROM \"t1\"\n" +
               "WHERE \"name\" = 'foo'\n"));
        assertThat(rows[1][0], is("t1_view2"));
        assertThat(rows[1][1],
            is("SELECT \"id\"\n" +
               "FROM \"t1\"\n" +
               "WHERE \"name\" = 'foo'\n"));

        execute("SELECT table_name, column_name " +
                "FROM information_schema.columns " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        assertThat(printedTable(response.rows()), is(""));

        // Clean up metadata that does not show up in information_schema any more
        execute("DROP VIEW t1_view1, t1_view2");
    }

    @Test
    public void testSearchInformationSchemaTablesRefresh() {
        execute("select * from information_schema.tables");
        assertEquals(49L, response.rowCount());

        execute("create table t4 (col1 integer, col2 string) with(number_of_replicas=0)");
        ensureYellow(getFqn("t4"));

        execute("select * from information_schema.tables");
        assertEquals(50L, response.rowCount());
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderBy() {
        String defaultSchema = sqlExecutor.getCurrentSchema();
        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        execute("create table foo (col1 integer primary key, " +
                "col2 string) clustered by(col1) into 3 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema = ? order by table_name asc",
            new Object[]{defaultSchema});
        assertThat(response.rowCount(), is(2L));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[0][13], is(defaultSchema));
        assertThat(response.rows()[0][12], is("foo"));
        assertThat(response.rows()[0][11], is(defaultSchema));
        assertThat(response.rows()[0][14], is("BASE TABLE"));
        assertThat(response.rows()[0][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        assertThat(response.rows()[0][5], is(3));
        assertThat(response.rows()[0][4], is("0-1"));
        assertThat(response.rows()[0][2], is("col1"));
        assertThat(response.rows()[0][1], is(false));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[1][13], is(defaultSchema));
        assertThat(response.rows()[1][12], is("test"));
        assertThat(response.rows()[1][11], is(defaultSchema));
        assertThat(response.rows()[1][14], is("BASE TABLE"));
        assertThat(response.rows()[1][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        assertThat(response.rows()[1][5], is(5));
        assertThat(response.rows()[1][4], is("0-1"));
        assertThat(response.rows()[1][2], is("col1"));
        assertThat(response.rows()[0][1], is(false));
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimit() {
        execute("create table test (col1 integer primary key, col2 string)");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select table_schema, table_name, number_of_shards, number_of_replicas " +
                "from INFORMATION_SCHEMA.Tables where table_schema = ? " +
                "order by table_name asc limit 1", new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(1L, response.rowCount());
        assertEquals(sqlExecutor.getCurrentSchema(), response.rows()[0][0]);
        assertEquals("foo", response.rows()[0][1]);
        assertEquals(3, response.rows()[0][2]);
        assertEquals("0-1", response.rows()[0][3]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByTwoColumnsAndLimit() {
        execute("create table test (col1 integer primary key, col2 string) clustered into 1 shards");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        execute("create table bar (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select table_name, number_of_shards from INFORMATION_SCHEMA.Tables where table_schema = ? " +
                "order by number_of_shards desc, table_name asc limit 2", new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(2L, response.rowCount());

        assertThat(printedTable(response.rows()), is(
            "bar| 3\n" +
            "foo| 3\n"));
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimitOffset() {
        String defaultSchema = sqlExecutor.getCurrentSchema();
        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema = ? order by table_name asc limit 1 offset 1", new Object[]{defaultSchema});
        assertThat(response.rowCount(), is(1L));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null); // version
        assertThat(response.rows()[0][13], is(defaultSchema)); // table_schema
        assertThat(response.rows()[0][12], is("test"));  // table_name
        assertThat(response.rows()[0][11], is(defaultSchema)); // table_catalog
        assertThat(response.rows()[0][14], is("BASE TABLE")); // table_type
        assertThat(response.rows()[0][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME)); // routing_hash_function
        assertThat(response.rows()[0][5], is(5)); // number_of_shards
        assertThat(response.rows()[0][4], is("0-1")); // number_of_replicas
        assertThat(response.rows()[0][2], is("col1")); // primary key
        assertThat(response.rows()[0][1], is(false)); // closed
    }

    @Test
    public void testSelectFromInformationSchemaTable() {
        execute("select TABLE_NAME from INFORMATION_SCHEMA.Tables where table_schema='doc'");
        assertEquals(0L, response.rowCount());

        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by from INFORMATION_SCHEMA.Tables where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals("0-1", response.rows()[0][2]);
        assertEquals("col1", response.rows()[0][3]);
    }

    @Test
    public void testSelectBlobTablesFromInformationSchemaTable() {
        execute("select TABLE_NAME from INFORMATION_SCHEMA.Tables where table_schema='blob'");
        assertEquals(0L, response.rowCount());

        String blobsPath = createTempDir().toAbsolutePath().toString();
        execute("create blob table test clustered into 5 shards with (blobs_path=?)", new Object[]{blobsPath});
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by, blobs_path, routing_hash_function, version " +
                "from INFORMATION_SCHEMA.Tables where table_schema='blob' ");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals("0-1", response.rows()[0][2]);
        assertEquals("digest", response.rows()[0][3]);
        assertEquals(blobsPath, response.rows()[0][4]);
        assertThat(response.rows()[0][5], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        TestingHelpers.assertCrateVersion(response.rows()[0][6], Version.CURRENT, null);

        // cleanup blobs path, tempDir hook will be deleted before table would be deleted, avoid error in log
        execute("drop blob table test");
    }

    @Test
    public void testSelectPartitionedTablesFromInformationSchemaTable() {
        execute("create table test (id int, name string, o object as (i int), primary key (id, o['i']))" +
                " partitioned by (o['i'])");
        execute("insert into test (id, name, o) values (1, 'Youri', {i=10}), (2, 'Ruben', {i=20})");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by, partitioned_by from INFORMATION_SCHEMA.Tables where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(4, response.rows()[0][1]);
        assertEquals("0-1", response.rows()[0][2]);
        assertEquals("_id", response.rows()[0][3]);
        assertThat((List<Object>) response.rows()[0][4], Matchers.contains("o['i']"));
    }

    @Test
    public void testSelectStarFromInformationSchemaTable() {
        execute("create table test (col1 integer, col2 string) clustered into 5 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount(), is(1L));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[0][13], is(sqlExecutor.getCurrentSchema()));
        assertThat(response.rows()[0][12], is("test"));
        assertThat(response.rows()[0][11], is(sqlExecutor.getCurrentSchema()));
        assertThat(response.rows()[0][14], is("BASE TABLE"));
        assertThat(response.rows()[0][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        assertThat(response.rows()[0][5], is(5));
        assertThat(response.rows()[0][4], is("0-1"));
        assertThat(response.rows()[0][2], is("_id"));
        assertThat(response.rows()[0][1], is(false));
    }

    @Test
    public void testSelectFromTableConstraints() throws Exception {
        execute("SELECT column_name FROM information_schema.columns WHERE table_schema='information_schema' " +
                "AND table_name='table_constraints'");
        assertEquals(9L, response.rowCount());
        assertThat(TestingHelpers.getColumn(response.rows(),0),
            arrayContaining("constraint_catalog", "constraint_name", "constraint_schema", "constraint_type", "initially_deferred",
                "is_deferrable", "table_catalog", "table_name", "table_schema"));
        execute("SELECT constraint_name, constraint_type, table_name, table_schema FROM " +
                "information_schema.table_constraints ORDER BY table_schema ASC, table_name ASC");
        assertThat(printedTable(response.rows()),
            is(
                "columns_pk| PRIMARY KEY| columns| information_schema\n" +
                "information_schema_columns_column_name_not_null| CHECK| columns| information_schema\n" +
                "information_schema_columns_data_type_not_null| CHECK| columns| information_schema\n" +
                "information_schema_columns_is_generated_not_null| CHECK| columns| information_schema\n" +
                "information_schema_columns_is_nullable_not_null| CHECK| columns| information_schema\n" +
                "information_schema_columns_ordinal_position_not_null| CHECK| columns| information_schema\n" +
                "information_schema_columns_table_catalog_not_null| CHECK| columns| information_schema\n" +
                "information_schema_columns_table_name_not_null| CHECK| columns| information_schema\n" +
                "information_schema_columns_table_schema_not_null| CHECK| columns| information_schema\n" +
                "key_column_usage_pk| PRIMARY KEY| key_column_usage| information_schema\n" +
                "referential_constraints_pk| PRIMARY KEY| referential_constraints| information_schema\n" +
                "schemata_pk| PRIMARY KEY| schemata| information_schema\n" +
                "sql_features_pk| PRIMARY KEY| sql_features| information_schema\n" +
                "table_constraints_pk| PRIMARY KEY| table_constraints| information_schema\n" +
                "table_partitions_pk| PRIMARY KEY| table_partitions| information_schema\n" +
                "tables_pk| PRIMARY KEY| tables| information_schema\n" +
                "views_pk| PRIMARY KEY| views| information_schema\n" +
                "allocations_pk| PRIMARY KEY| allocations| sys\n" +
                "checks_pk| PRIMARY KEY| checks| sys\n" +
                "jobs_pk| PRIMARY KEY| jobs| sys\n" +
                "jobs_log_pk| PRIMARY KEY| jobs_log| sys\n" +
                "node_checks_pk| PRIMARY KEY| node_checks| sys\n" +
                "nodes_pk| PRIMARY KEY| nodes| sys\n" +
                "privileges_pk| PRIMARY KEY| privileges| sys\n" +
                "repositories_pk| PRIMARY KEY| repositories| sys\n" +
                "shards_pk| PRIMARY KEY| shards| sys\n" +
                "snapshot_restore_pk| PRIMARY KEY| snapshot_restore| sys\n" +
                "snapshots_pk| PRIMARY KEY| snapshots| sys\n" +
                "summits_pk| PRIMARY KEY| summits| sys\n" +
                "users_pk| PRIMARY KEY| users| sys\n"
            ));

        execute("CREATE TABLE test (\n" +
                " col1 INTEGER constraint chk_1 check (col1 > 10),\n" +
                " col2 INTEGER,\n" +
                " col3 INT NOT NULL,\n" +
                " col4 STRING,\n" +
                " PRIMARY KEY(col1,col2),\n" +
                " CONSTRAINT unnecessary_check CHECK (col2 != col1)\n" +
                ")");
        ensureGreen();
        execute("SELECT constraint_type, constraint_name, table_name FROM information_schema.table_constraints " +
                "WHERE table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(4L, response.rowCount());
        assertThat(response.rows()[0][0], is("PRIMARY KEY"));
        assertThat(response.rows()[0][1], is("test_pk"));
        assertThat(response.rows()[0][2], is("test"));
        assertThat(response.rows()[1][0], is("CHECK"));
        assertThat(response.rows()[1][1], is(sqlExecutor.getCurrentSchema() + "_test_col3_not_null"));
        assertThat(response.rows()[1][2], is("test"));
        assertThat(response.rows()[2][0], is("CHECK"));
        assertThat(response.rows()[2][1], is("unnecessary_check"));
        assertThat(response.rows()[2][2], is("test"));
        assertThat(response.rows()[3][0], is("CHECK"));
        assertThat(response.rows()[3][1], is("chk_1"));
        assertThat(response.rows()[3][2], is("test"));
    }

    @Test
    public void testRefreshTableConstraints() {
        execute("CREATE TABLE test (col1 INTEGER PRIMARY KEY, col2 STRING)");
        ensureGreen();
        execute("SELECT table_name, constraint_name FROM Information_schema" +
                ".table_constraints WHERE table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(1L, response.rowCount());
        assertThat(response.rows()[0][0], is("test"));
        assertThat(response.rows()[0][1], is("test_pk"));

        execute("CREATE TABLE test2 (" +
            "   col1a STRING PRIMARY KEY," +
            "   \"Col2a\" TIMESTAMP WITH TIME ZONE NOT NULL)");
        ensureGreen();
        execute("SELECT table_name, constraint_name FROM information_schema.table_constraints WHERE table_schema = ? " +
                "ORDER BY table_name ASC",
            new Object[]{sqlExecutor.getCurrentSchema()});

        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[1][0], is("test2"));
        assertThat(response.rows()[1][1], is("test2_pk"));
        assertThat(response.rows()[2][0], is("test2"));
        assertThat(response.rows()[2][1], is(sqlExecutor.getCurrentSchema() + "_test2_Col2a_not_null"));
    }


    @Test
    public void testSelectAnalyzersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='ANALYZER' order by " +
                "routine_name desc limit 5");
        assertThat(printedTable(response.rows()), is(
            "whitespace\n" +
            "stop\n" +
            "standard\n" +
            "simple\n" +
            "keyword\n"
        ));
    }

    @Test
    public void testSelectTokenizersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='TOKENIZER' order by " +
                "routine_name asc limit 5");
        assertThat(printedTable(response.rows()), is("standard\n"));
    }

    @Test
    public void testSelectTokenFiltersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='TOKEN_FILTER' order by " +
                "routine_name asc limit 5");
        assertThat(printedTable(response.rows()), is(
            "cjk_bigram\n" +
            "cjk_width\n" +
            "delimited_payload_filter\n" +
            "dictionary_decompounder\n" +
            "hunspell\n"
        ));
    }

    @Test
    public void testSelectCharFiltersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='CHAR_FILTER' order by " +
                "routine_name asc");
        assertThat(printedTable(response.rows()), is(
            "mapping\n" +
            "pattern_replace\n"
        ));
    }

    @Test
    public void testTableConstraintsWithOrderBy() {
        execute("create table test1 (col11 integer primary key, col12 float)");
        execute("create table test2 (col21 double primary key, col22 string)");
        execute("create table abc (col31 integer primary key, col32 string)");

        ensureGreen();

        execute("select table_name from INFORMATION_SCHEMA.table_constraints " +
                "where table_schema not in ('sys', 'information_schema')  " +
                "ORDER BY table_name");
        assertEquals(3L, response.rowCount());
        assertEquals("abc", response.rows()[0][0]);
        assertEquals("test1", response.rows()[1][0]);
        assertEquals("test2", response.rows()[2][0]);
    }

    @Test
    public void testDefaultColumns() {
        execute("select * from information_schema.columns order by table_schema, table_name");
        assertEquals(874, response.rowCount());
    }

    @Test
    public void testColumnsColumns() {
        execute("select column_name, data_type from information_schema.columns where table_schema='information_schema' and table_name='columns' order by column_name asc");
        assertThat(response.rowCount(), is(35L));
        assertThat(printedTable(response.rows()), is(
            "character_maximum_length| integer\n" +
            "character_octet_length| integer\n" +
            "character_set_catalog| text\n" +
            "character_set_name| text\n" +
            "character_set_schema| text\n" +
            "check_action| integer\n" +
            "check_references| text\n" +
            "collation_catalog| text\n" +
            "collation_name| text\n" +
            "collation_schema| text\n" +
            "column_default| text\n" +
            "column_details| object\n" +
            "column_details['name']| text\n" +
            "column_details['path']| text_array\n" +
            "column_name| text\n" +
            "data_type| text\n" +
            "datetime_precision| integer\n" +
            "domain_catalog| text\n" +
            "domain_name| text\n" +
            "domain_schema| text\n" +
            "generation_expression| text\n" +
            "interval_precision| integer\n" +
            "interval_type| text\n" +
            "is_generated| text\n" +
            "is_nullable| boolean\n" +
            "numeric_precision| integer\n" +
            "numeric_precision_radix| integer\n" +
            "numeric_scale| integer\n" +
            "ordinal_position| integer\n" +
            "table_catalog| text\n" +
            "table_name| text\n" +
            "table_schema| text\n" +
            "udt_catalog| text\n" +
            "udt_name| text\n" +
            "udt_schema| text\n")
        );
    }

    @Test
    public void testSupportedPgTypeColumns() {
        execute("select column_name, data_type from information_schema.columns " +
                "where table_schema = 'pg_catalog' " +
                "and table_name = 'pg_type' " +
                "order by 1 asc");
        assertThat(printedTable(response.rows()), is(
            "oid| integer\n" +
            "typarray| integer\n" +
            "typbasetype| integer\n" +
            "typbyval| boolean\n" +
            "typcategory| text\n" +
            "typcollation| integer\n" +
            "typdefault| text\n" +
            "typdelim| text\n" +
            "typelem| integer\n" +
            "typinput| regproc\n" +
            "typisdefined| boolean\n" +
            "typlen| smallint\n" +
            "typname| text\n" +
            "typnamespace| integer\n" +
            "typndims| integer\n" +
            "typnotnull| boolean\n" +
            "typoutput| regproc\n" +
            "typowner| integer\n" +
            "typreceive| regproc\n" +
            "typrelid| integer\n" +
            "typtype| text\n" +
            "typtypmod| integer\n")
        );
    }

    @Test
    public void testSelectFromTableColumns() {
        String defaultSchema = sqlExecutor.getCurrentSchema();
        execute("create table test ( " +
                "col1 integer primary key, " +
                "col2 string index off, " +
                "age integer not null, " +
                "b byte, " +
                "s short, " +
                "d double, " +
                "f float, " +
                "stuff object(dynamic) AS (" +
                "  level1 object(dynamic) AS (" +
                "    level2 string not null," +
                "    level2_nullable string" +
                "  ) not null" +
                ") not null)");

        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Columns where table_schema = ? order by column_name asc", new Object[]{defaultSchema});
        assertEquals(11L, response.rowCount());
        assertEquals("age", response.rows()[0][12]); // column_name
        assertEquals("integer", response.rows()[0][13]); // data_type
        assertEquals(null, response.rows()[0][14]); // datetime_precision
        assertEquals("NEVER", response.rows()[0][21]); // is_generated
        assertEquals(false, response.rows()[0][22]); // is_nullable
        assertEquals(32, response.rows()[0][23]); // numeric_precision
        assertEquals(2, response.rows()[0][24]); // numeric_precision_radix

        assertEquals(3, response.rows()[0][26]); // ordinal_position
        assertEquals(defaultSchema, response.rows()[0][27]); // table_catalog
        assertEquals("test", response.rows()[0][28]); // table_name
        assertEquals(defaultSchema, response.rows()[0][29]); // table_schema

        assertEquals("col1", response.rows()[2][12]);
        assertEquals(false, response.rows()[2][22]);

        assertEquals("col2", response.rows()[3][12]);
        assertEquals(true, response.rows()[3][22]);

        assertEquals("b", response.rows()[1][12]);
        assertEquals(8, response.rows()[1][23]);
        assertEquals("s", response.rows()[6][12]);
        assertEquals(16, response.rows()[6][23]);
        assertEquals("d", response.rows()[4][12]);
        assertEquals(53, response.rows()[4][23]);
        assertEquals("f", response.rows()[5][12]);
        assertEquals(24, response.rows()[5][23]);
        assertEquals("stuff", response.rows()[7][12]);
        assertEquals(false, response.rows()[7][22]); // is_nullable
        assertEquals("stuff['level1']", response.rows()[8][12]);
        assertEquals(false, response.rows()[8][22]); // is_nullable
        assertEquals("stuff['level1']['level2']", response.rows()[9][12]);
        assertEquals(false, response.rows()[9][22]); // is_nullable
        assertEquals("stuff['level1']['level2_nullable']", response.rows()[10][12]);
        assertEquals(true, response.rows()[10][22]); // is_nullable

        assertEquals(Map.of("name","stuff","path", List.of()) , response.rows()[7][11]); // column_details
        assertEquals(Map.of("name","stuff","path", List.of("level1")) , response.rows()[8][11]); // column_details
        assertEquals(Map.of("name","stuff","path", List.of("level1","level2")) , response.rows()[9][11]); // column_details
        assertEquals(Map.of("name","stuff","path", List.of("level1","level2_nullable")) , response.rows()[10][11]); // column_details
    }

    @Test
    public void testSelectFromTableColumnsRefresh() {
        execute("create table test (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(3L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);

        execute("create table test2 (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns " +
                "where table_schema = ? " +
                "order by table_name", new Object[]{sqlExecutor.getCurrentSchema()});

        assertEquals(6L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals("test2", response.rows()[4][0]);
    }

    @Test
    public void testSelectFromTableColumnsMultiField() {
        execute("create table test (col1 string, col2 string," +
                "index col1_col2_ft using fulltext(col1, col2))");
        ensureGreen();
        execute("select table_name, column_name," +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(2L, response.rowCount());

        assertEquals("test", response.rows()[0][0]);
        assertEquals("col1", response.rows()[0][1]);
        assertEquals(1, response.rows()[0][2]);
        assertEquals("text", response.rows()[0][3]);

        assertEquals("test", response.rows()[1][0]);
        assertEquals("col2", response.rows()[1][1]);
        assertEquals(2, response.rows()[1][2]);
        assertEquals("text", response.rows()[1][3]);
    }

    @Test
    public void testGlobalAggregation() {
        execute("select max(ordinal_position) from information_schema.columns");
        assertEquals(1, response.rowCount());

        assertEquals(119, response.rows()[0][0]);

        execute("create table t1 (id integer, col1 string)");
        ensureGreen();
        execute("select max(ordinal_position) from information_schema.columns where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(1, response.rowCount());

        assertEquals(2, response.rows()[0][0]);
    }

    @Test
    public void testGlobalAggregationMany() {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=0)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=0)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=0)");
        ensureYellow();
        execute("select min(number_of_shards), max(number_of_shards), avg(number_of_shards)," +
                "sum(number_of_shards) from information_schema.tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(1, response.rowCount());

        assertEquals(3, response.rows()[0][0]);
        assertEquals(10, response.rows()[0][1]);
        assertEquals(6.0d, response.rows()[0][2]);
        assertEquals(18L, response.rows()[0][3]);
    }

    @Test
    public void testGlobalAggregationWithWhere() {
        execute("create table t1 (id integer, col1 string) clustered into 1 shards with(number_of_replicas=0)");
        execute("create table t2 (id integer, col1 string) clustered into 2 shards with(number_of_replicas=0)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=0)");
        ensureYellow();
        execute("select min(number_of_shards), max(number_of_shards), avg(number_of_shards)," +
                "sum(number_of_shards) from information_schema.tables where table_schema = ? and table_name != 't1'",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertEquals(1, response.rowCount());

        assertEquals(2, response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals(2.5d, response.rows()[0][2]);
        assertEquals(5L, response.rows()[0][3]);
    }

    @Test
    public void testGlobalAggregationWithAlias() {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=0)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=0)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=0)");
        ensureYellow();
        execute("select min(number_of_shards) as min_shards from information_schema.tables where table_name = 't1'");
        assertEquals(1, response.rowCount());

        assertEquals(10, response.rows()[0][0]);
    }

    @Test
    public void testGlobalCount() {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=0)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=0)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=0)");
        execute("select count(*) from information_schema.tables");
        assertEquals(1, response.rowCount());
        assertEquals(52L, response.rows()[0][0]);
    }

    @Test
    public void testGlobalCountDistinct() {
        execute("create table t3 (TableInfoid integer, col1 string)");
        ensureGreen();
        execute("select count(distinct table_schema) from information_schema.tables order by count(distinct table_schema)");
        assertEquals(1, response.rowCount());
        assertEquals(4L, response.rows()[0][0]);
    }

    @Test
    public void selectDynamicObjectAddsSubColumn() throws Exception {
        execute("create table t4 (" +
                "  title string," +
                "  stuff object(dynamic) as (" +
                "    first_name string," +
                "    last_name string" +
                "  )" +
                ") with (number_of_replicas=0)");
        ensureGreen();
        execute("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(4, response.rowCount());

        execute("insert into t4 (stuff) values (?)", new Object[]{
            new HashMap<String, Object>() {{
                put("first_name", "Douglas");
                put("middle_name", "Noel");
                put("last_name", "Adams");
            }}
        });
        execute("refresh table t4");

        waitForMappingUpdateOnAll("t4", "stuff.first_name", "stuff.middle_name", "stuff.last_name");
        execute("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(5, response.rowCount());
    }

    @Test
    public void testAddColumnToIgnoredObject() {
        execute("create table t4 (" +
                "  title string," +
                "  stuff object(ignored) as (" +
                "    first_name string," +
                "    last_name string" +
                "  )" +
                ")");
        ensureYellow();
        execute("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(4, response.rowCount());

        execute("insert into t4 (stuff) values (?)", new Object[]{
            new HashMap<String, Object>() {{
                put("first_name", "Douglas");
                put("middle_name", "Noel");
                put("last_name", "Adams");
            }}
        });

        execute("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(4, response.rowCount());
    }

    @Test
    public void testPartitionedBy() {
        execute("create table my_table (id integer, name string) partitioned by (name)");
        execute("create table my_other_table (id integer, name string, content string) " +
                "partitioned by (name, content)");

        execute("select * from information_schema.tables " +
                "where table_schema = ? order by table_name", new Object[]{sqlExecutor.getCurrentSchema()});

        assertThat((List<Object>) response.rows()[0][6], Matchers.contains("name", "content"));
        assertThat((List<Object>) response.rows()[1][6], Matchers.contains("name"));
    }

    @Test
    public void testTablePartitions() {
        execute("create table my_table (par int, content string) " +
                "clustered into 5 shards " +
                "partitioned by (par)");
        execute("insert into my_table (par, content) values (1, 'content1')");
        execute("insert into my_table (par, content) values (1, 'content2')");
        execute("insert into my_table (par, content) values (2, 'content3')");
        execute("insert into my_table (par, content) values (2, 'content4')");
        execute("insert into my_table (par, content) values (2, 'content5')");
        execute("insert into my_table (par, content) values (3, 'content6')");
        ensureGreen();

        execute("select table_name," +
                "   table_schema, " +
                "   partition_ident, " +
                "   values, " +
                "   values['par'], " +
                "   number_of_shards, " +
                "   number_of_replicas, " +
                "   settings['write']['wait_for_active_shards'] " +
                "from information_schema.table_partitions order by table_name, partition_ident");
        assertEquals(3, response.rowCount());

        Object[] row1 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "04132", Map.of("par", 1), "1", 5, "0-1", "1"};
        Object[] row2 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "04134", Map.of("par", 2), "2", 5, "0-1", "1"};
        Object[] row3 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "04136", Map.of("par", 3), "3", 5, "0-1", "1"};

        assertArrayEquals(row1, response.rows()[0]);
        assertArrayEquals(row2, response.rows()[1]);
        assertArrayEquals(row3, response.rows()[2]);
    }

    @Test
    public void testPartitionedTableShardsAndReplicas() throws Exception {
        execute("create table parted (par byte, content string) " +
                "partitioned by (par) " +
                "clustered into 2 shards with (number_of_replicas=0, column_policy='dynamic')");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(printedTable(response.rows()), is("parted| 2| 0\n"));

        execute("select * from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount(), is(0L));

        execute("insert into parted (par, content) values (1, 'foo'), (3, 'baz')");
        execute("refresh table parted");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(printedTable(response.rows()), is("parted| 2| 0\n"));

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(printedTable(response.rows()), is(
            "parted| 04132| {par=1}| 2| 0\n" +
            "parted| 04136| {par=3}| 2| 0\n"));

        execute("alter table parted set (number_of_shards=6)");
        waitNoPendingTasksOnAll();

        execute("insert into parted (par, content) values (2, 'bar')");
        execute("refresh table parted");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(printedTable(response.rows()), is("parted| 6| 0\n"));

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount(), is(3L));
        assertThat(printedTable(response.rows()), is(
            "parted| 04132| {par=1}| 2| 0\n" +
            "parted| 04134| {par=2}| 6| 0\n" +
            "parted| 04136| {par=3}| 2| 0\n"));

        execute("update parted set new=true where par=1");
        refresh();
        waitNoPendingTasksOnAll();

        // ensure newer index metadata does not override settings in template
        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(printedTable(response.rows()), is("parted| 6| 0\n"));

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount(), is(3L));
        assertThat(printedTable(response.rows()), is(
            "parted| 04132| {par=1}| 2| 0\n" +
            "parted| 04134| {par=2}| 6| 0\n" +
            "parted| 04136| {par=3}| 2| 0\n"));

        execute("delete from parted where par=2");
        waitNoPendingTasksOnAll();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(printedTable(response.rows()), is("parted| 6| 0\n"));
        waitNoPendingTasksOnAll();
        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat(printedTable(response.rows()), is(
            "parted| 04132| {par=1}| 2| 0\n" +
            "parted| 04136| {par=3}| 2| 0\n"));


    }

    @Test
    public void testTablePartitionsMultiCol() throws Exception {
        execute("create table my_table (par int, par_str string, content string) " +
                "clustered into 5 shards " +
                "partitioned by (par, par_str)");
        execute("insert into my_table (par, par_str, content) values (1, 'foo', 'content1')");
        execute("insert into my_table (par, par_str, content) values (1, 'bar', 'content2')");
        execute("insert into my_table (par, par_str, content) values (2, 'foo', 'content3')");
        execute("insert into my_table (par, par_str, content) values (2, 'bar', 'content4')");
        ensureGreen();
        refresh();
        execute("alter table my_table set (number_of_shards=4)");
        waitNoPendingTasksOnAll();
        execute("insert into my_table (par, par_str, content) values (2, 'asdf', 'content5')");
        ensureYellow();

        execute("select table_name, table_schema, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions order by table_name, partition_ident");
        assertEquals(5, response.rowCount());

        Object[] row1 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "08132132c5p0", Map.of("par", 1, "par_str", "bar"), 5, "0-1"};
        Object[] row2 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "08132136dtng", Map.of("par", 1, "par_str", "foo"), 5, "0-1"};
        Object[] row3 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "08134132c5p0", Map.of("par", 2, "par_str", "bar"), 5, "0-1"};
        Object[] row4 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "08134136dtng", Map.of("par", 2, "par_str", "foo"), 5, "0-1"};
        Object[] row5 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "081341b1edi6c", Map.of("par", 2, "par_str", "asdf"), 4, "0-1"};

        assertArrayEquals(row1, response.rows()[0]);
        assertArrayEquals(row2, response.rows()[1]);
        assertArrayEquals(row3, response.rows()[2]);
        assertArrayEquals(row4, response.rows()[3]);
        assertArrayEquals(row5, response.rows()[4]);
    }

    @Test
    public void testPartitionsNestedCol() {
        execute(
            "create table my_table (" +
            "   id int," +
            "   metadata object as (date timestamp with time zone)" +
            ") clustered into 5 shards " +
            "partitioned by (metadata['date'])");
        ensureYellow();
        execute("insert into my_table (id, metadata) values (?, ?), (?, ?)",
            new Object[]{
                1, Map.of("date", "1970-01-01"),
                2, Map.of("date", "2014-05-28")
            });
        refresh();

        execute("select table_name, partition_ident, values from information_schema.table_partitions order by table_name, partition_ident");
        assertEquals(2, response.rowCount());

        assertThat(printedTable(response.rows()),
            is("my_table| 04130| {metadata['date']=0}\n" +
               "my_table| 04732d1g64p36d9i60o30c1g| {metadata['date']=1401235200000}\n"));
    }

    @Test
    public void testAnyInformationSchema() {
        execute(
            "create table any1 (" +
            "   id integer," +
            "   date timestamp with time zone," +
            "   names array(string)" +
            ") partitioned by (date)");
        execute("create table any2 (id integer, num long, names array(string)) partitioned by (num)");
        ensureGreen();
        execute("select table_name from information_schema.tables where 'date' = ANY (partitioned_by)");
        assertThat(response.rowCount(), is(1L));
        assertThat(response.rows()[0][0], is("any1"));
    }

    @Test
    public void testDynamicObjectPartitionedTableInformationSchemaColumns() throws Exception {
        String stmtCreate = "create table data_points (" +
                            "day string primary key," +
                            "data object(dynamic)" +
                            ") partitioned by (day)";
        execute(stmtCreate);

        String stmtInsert = "insert into data_points (day, data) values (?, ?)";
        Map<String, Object> obj = new HashMap<>();
        obj.put("somestringroute", "stringvalue");
        obj.put("somelongroute", 1338L);
        Object[] argsInsert = new Object[]{
            "20140520",
            obj
        };
        execute(stmtInsert, argsInsert);
        assertThat(response.rowCount(), is(1L));
        waitForMappingUpdateOnAll("data_points", "data.somestringroute");
        refresh();

        String stmtIsColumns = "select table_name, column_name, data_type " +
                               "from information_schema.columns " +
                               "where table_name = 'data_points' " +
                               "order by column_name";
        execute(stmtIsColumns);
        assertThat(response.rowCount(), is(4L));

        String expected = "data_points| data| object\n" +
                          "data_points| data['somelongroute']| bigint\n" +
                          "data_points| data['somestringroute']| text\n" +
                          "data_points| day| text\n";
        assertEquals(expected, printedTable(response.rows()));
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testRegexpMatch() {
        execute("create blob table blob_t1");
        execute("create table t(id String)");
        ensureYellow();
        execute("select distinct table_schema from information_schema.tables " +
                "where table_schema ~ '[a-z]+o[a-z]' order by table_schema");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows()[0][0], is("blob"));
        assertThat(response.rows()[1][0], is("doc"));
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testSelectSchemata() throws Exception {
        execute("select * from information_schema.schemata order by schema_name asc");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.getColumn(response.rows(), 0), is(Matchers.arrayContaining("blob", "doc", "information_schema", "pg_catalog", "sys")));

        execute("create table t1 (col string) with (number_of_replicas=0)");
        ensureGreen();

        execute("select * from information_schema.schemata order by schema_name asc");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.getColumn(response.rows(), 0), is(Matchers.arrayContaining("blob", "doc", "information_schema", "pg_catalog", "sys")));
    }

    @Test
    public void testSelectGeneratedColumnFromInformationSchemaColumns() {
        execute("create table t (lastname string, firstname string, name as (lastname || '_' || firstname)) " +
                "with (number_of_replicas = 0)");
        execute("select column_name, is_generated, generation_expression from information_schema.columns where is_generated = 'ALWAYS'");
        assertThat(printedTable(response.rows()),
            is("name| ALWAYS| concat(concat(lastname, '_'), firstname)\n"));
    }

    @Test
    public void testSelectSqlFeatures() {
        execute("select * from information_schema.sql_features order by feature_id asc");
        assertThat(response.rowCount(), is(672L));

        execute("select feature_id, feature_name from information_schema.sql_features where feature_id='E011'");
        assertThat(response.rowCount(), is(7L));
        assertThat(response.rows()[0][1], is("Numeric data types"));
    }

    @Test
    public void testScalarEvaluatesInErrorOnInformationSchema() {
        assertThrowsMatches(() -> execute("select 1/0 from information_schema.tables"),
                     isSQLError(is("/ by zero"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));
    }

    @Test
    public void testInformationRoutinesColumns() {
        execute("select column_name from information_schema.columns where table_name='routines' order by column_name asc");
        assertThat(printedTable(response.rows()),
            is(
              "data_type\n" +
              "is_deterministic\n" +
              "routine_body\n" +
              "routine_definition\n" +
              "routine_name\n" +
              "routine_schema\n" +
              "routine_type\n" +
              "specific_name\n"
            ));
    }

    @Test
    public void testOpenCloseTableInformation() {
        execute("create table t (i int)");
        ensureYellow();

        execute("alter table t close");
        execute("select closed from information_schema.tables where table_name = 't'");
        assertEquals(1, response.rowCount());
        assertEquals(true, response.rows()[0][0]);

        execute("alter table t open");
        execute("select closed from information_schema.tables where table_name = 't'");
        assertEquals(1, response.rowCount());
        assertEquals(false, response.rows()[0][0]);
    }

    @Test
    public void testOpenClosePartitionInformation() {
        execute("create table t (i int) partitioned by (i)");
        ensureYellow();

        execute("insert into t values (1)");
        String partitionIdent = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "t"),
            Collections.singletonList("1")
        ).ident();

        execute("insert into t values (2), (3)");
        ensureYellow();

        execute("select closed from information_schema.table_partitions where table_name = 't'");

        assertEquals(3, response.rowCount());
        assertEquals(false, response.rows()[0][0]);
        assertEquals(false, response.rows()[1][0]);
        assertEquals(false, response.rows()[2][0]);

        execute("alter table t partition (i = 1) close");
        execute("select partition_ident, values from information_schema.table_partitions" +
                " where table_name = 't' and closed = true");

        assertEquals(1, response.rowCount());

        HashMap values = (HashMap) response.rows()[0][1];
        assertEquals(1, values.get("i"));
        assertTrue(partitionIdent.endsWith((String) response.rows()[0][0]));

        execute("alter table t partition (i = 1) open");
        execute("select closed from information_schema.table_partitions");

        assertEquals(3, response.rowCount());
        assertEquals(false, response.rows()[0][0]);
        assertEquals(false, response.rows()[1][0]);
        assertEquals(false, response.rows()[2][0]);
    }

    @Test
    public void testSelectFromKeyColumnUsage() {
        execute("create table table1 (id1 integer)");
        execute("create table table2 (id2 integer primary key)");
        execute("create table table3 (id3 integer, name string, other double, primary key (id3, name))");
        ensureYellow();

        execute("select * from information_schema.key_column_usage order by table_name, ordinal_position asc");
        assertEquals(3L, response.rowCount());
        assertThat(response.cols(), arrayContaining(
            "column_name",
            "constraint_catalog",
            "constraint_name",
            "constraint_schema",
            "ordinal_position",
            "table_catalog",
            "table_name",
            "table_schema"
        ));

        final String defaultSchema = sqlExecutor.getCurrentSchema();
        Matcher<Object[][]> resultMatcher = arrayContaining(
            new Object[]{"id2", defaultSchema, "table2_pk", defaultSchema, 1, defaultSchema, "table2", defaultSchema},
            new Object[]{"id3", defaultSchema, "table3_pk", defaultSchema, 1, defaultSchema, "table3", defaultSchema},
            new Object[]{"name", defaultSchema, "table3_pk", defaultSchema, 2, defaultSchema, "table3", defaultSchema}
        );
        assertThat(response.rows(), resultMatcher);

        // check that the constraint name is the same as in table_constraints by joining the two tables
        execute("select t1.* from information_schema.key_column_usage t1 " +
                "join information_schema.table_constraints t2 " +
                "on t1.constraint_name = t2.constraint_name " +
                "order by t1.table_name, t1.ordinal_position asc");
        assertThat(response.rows(), resultMatcher);
    }

    @Test
    public void testSelectFromReferentialConstraints() {
        execute("select * from information_schema.referential_constraints");
        assertEquals(0L, response.rowCount());
        assertThat(response.cols(),
            arrayContaining("constraint_catalog", "constraint_name", "constraint_schema", "delete_rule",
                "match_option", "unique_constraint_catalog", "unique_constraint_name", "unique_constraint_schema",
                "update_rule"));
    }

    @Test
    public void test_character_maximum_length_information_schema_columns() {
        execute("CREATE TABLE t (col1 varchar, col2 varchar(1)) CLUSTERED INTO 1 SHARDS");
        execute("SELECT column_name, character_maximum_length " +
                "FROM information_schema.columns " +
                "WHERE table_name = 't'");
        assertThat(printedTable(response.rows()), is("col1| NULL\n" +
                                                     "col2| 1\n"));
    }
}
