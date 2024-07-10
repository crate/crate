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

import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThat;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.Version;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.metadata.IndexMappings;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.testing.Asserts;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseRandomizedSchema;

@IntegTestCase.ClusterScope(numDataNodes = 2)
public class InformationSchemaTest extends IntegTestCase {

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
        assertThat(response).hasRows(
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| character_sets| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| columns| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| foreign_server_options| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| foreign_servers| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| foreign_table_options| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| foreign_tables| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| key_column_usage| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| referential_constraints| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| routines| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| schemata| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| sql_features| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| table_constraints| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| table_partitions| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| tables| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| user_mapping_options| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| user_mappings| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| views| information_schema| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_am| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_attrdef| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_attribute| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_class| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_constraint| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_cursors| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_database| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_depend| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_description| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_enum| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_event_trigger| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_index| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_indexes| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_locks| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_matviews| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_namespace| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_proc| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_publication| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_publication_tables| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_range| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_roles| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_settings| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_shdescription| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_stats| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_subscription| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_subscription_rel| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_tables| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_tablespace| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_type| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| pg_views| pg_catalog| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| allocations| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| checks| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| cluster| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| health| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| jobs| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| jobs_log| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| jobs_metrics| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| node_checks| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| nodes| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| operations| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| operations_log| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| privileges| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| repositories| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| roles| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| segments| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| shards| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| snapshot_restore| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| snapshots| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| summits| sys| BASE TABLE| NULL",
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| crate| users| sys| BASE TABLE| NULL"
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
        assertThat(response).hasRows(
            "BASE TABLE| t1| 2| 1",
            "VIEW| t1_view1| NULL| NULL",
            "VIEW| t1_view2| NULL| NULL");

        // SELECT information_schema.views
        execute("SELECT table_name, view_definition " +
                "FROM information_schema.views " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        Object[][] rows = response.rows();
        assertThat(rows[0][0]).isEqualTo("t1_view1");
        assertThat(rows[0][1]).isEqualTo(
            "SELECT *\n" +
            "FROM \"t1\"\n" +
            "WHERE \"name\" = 'foo'\n");
        assertThat(rows[1][0]).isEqualTo("t1_view2");
        assertThat(rows[1][1]).isEqualTo(
            "SELECT \"id\"\n" +
            "FROM \"t1\"\n" +
            "WHERE \"name\" = 'foo'\n");

        // SELECT information_schema.columns
        execute("SELECT table_name, column_name " +
                "FROM information_schema.columns " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        assertThat(response).hasRows("t1| id",
                                                                    "t1| name",
                                                                    "t1_view1| id",
                                                                    "t1_view1| name",
                                                                    "t1_view2| id");

        // After dropping the target table of the view, the view still shows up in information_schema.tables and
        // information_schema.views,  but not in information_schema.columns, because the SELECT statement could not be
        // analyzed.
        execute("DROP TABLE t1");

        execute("SELECT table_name, view_definition " +
                "FROM information_schema.views " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        assertThat(rows[0][0]).isEqualTo("t1_view1");
        assertThat(rows[0][1]).isEqualTo(
            "SELECT *\n" +
            "FROM \"t1\"\n" +
            "WHERE \"name\" = 'foo'\n");
        assertThat(rows[1][0]).isEqualTo("t1_view2");
        assertThat(rows[1][1]).isEqualTo(
            "SELECT \"id\"\n" +
            "FROM \"t1\"\n" +
            "WHERE \"name\" = 'foo'\n");

        execute("SELECT table_name, column_name " +
                "FROM information_schema.columns " +
                "WHERE table_name LIKE 't1%' " +
                "ORDER BY 1, 2");
        assertThat(response).isEmpty();

        // Clean up metadata that does not show up in information_schema any more
        execute("DROP VIEW t1_view1, t1_view2");
    }

    @Test
    public void testSearchInformationSchemaTablesRefresh() {
        execute("select * from information_schema.tables");
        assertThat(response.rowCount()).isEqualTo(67L);

        execute("create table t4 (col1 integer, col2 string) with(number_of_replicas=0)");
        ensureYellow(getFqn("t4"));

        execute("select * from information_schema.tables");
        assertThat(response.rowCount()).isEqualTo(68L);
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
        assertThat(response.rowCount()).isEqualTo(2L);

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[0][13]).isEqualTo(defaultSchema);
        assertThat(response.rows()[0][12]).isEqualTo("foo");
        assertThat(response.rows()[0][11]).isEqualTo("crate");
        assertThat(response.rows()[0][14]).isEqualTo("BASE TABLE");
        assertThat(response.rows()[0][8]).isEqualTo(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME);
        assertThat(response.rows()[0][5]).isEqualTo(3);
        assertThat(response.rows()[0][4]).isEqualTo("0-1");
        assertThat(response.rows()[0][2]).isEqualTo("col1");
        assertThat(response.rows()[0][1]).isEqualTo(false);

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[1][13]).isEqualTo(defaultSchema);
        assertThat(response.rows()[1][12]).isEqualTo("test");
        assertThat(response.rows()[1][11]).isEqualTo("crate");
        assertThat(response.rows()[1][14]).isEqualTo("BASE TABLE");
        assertThat(response.rows()[1][8]).isEqualTo(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME);
        assertThat(response.rows()[1][5]).isEqualTo(5);
        assertThat(response.rows()[1][4]).isEqualTo("0-1");
        assertThat(response.rows()[1][2]).isEqualTo("col1");
        assertThat(response.rows()[0][1]).isEqualTo(false);
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimit() {
        execute("create table test (col1 integer primary key, col2 string)");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select table_schema, table_name, number_of_shards, number_of_replicas " +
                "from INFORMATION_SCHEMA.Tables where table_schema = ? " +
                "order by table_name asc limit 1", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo(sqlExecutor.getCurrentSchema());
        assertThat(response.rows()[0][1]).isEqualTo("foo");
        assertThat(response.rows()[0][2]).isEqualTo(3);
        assertThat(response.rows()[0][3]).isEqualTo("0-1");
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByTwoColumnsAndLimit() {
        execute("create table test (col1 integer primary key, col2 string) clustered into 1 shards");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        execute("create table bar (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select table_name, number_of_shards from INFORMATION_SCHEMA.Tables where table_schema = ? " +
                "order by number_of_shards desc, table_name asc limit 2", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L);

        assertThat(response).hasRows(
            "bar| 3",
            "foo| 3");
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimitOffset() {
        String defaultSchema = sqlExecutor.getCurrentSchema();
        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema = ? order by table_name asc limit 1 offset 1", new Object[]{defaultSchema});
        assertThat(response.rowCount()).isEqualTo(1L);

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null); // version
        assertThat(response.rows()[0][13]).isEqualTo(defaultSchema); // table_schema
        assertThat(response.rows()[0][12]).isEqualTo("test");  // table_name
        assertThat(response.rows()[0][11]).isEqualTo("crate"); // table_catalog
        assertThat(response.rows()[0][14]).isEqualTo("BASE TABLE"); // table_type
        assertThat(response.rows()[0][8]).isEqualTo(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME); // routing_hash_function
        assertThat(response.rows()[0][5]).isEqualTo(5); // number_of_shards
        assertThat(response.rows()[0][4]).isEqualTo("0-1"); // number_of_replicas
        assertThat(response.rows()[0][2]).isEqualTo("col1"); // primary key
        assertThat(response.rows()[0][1]).isEqualTo(false); // closed
    }

    @Test
    public void testSelectFromInformationSchemaTable() {
        execute("select TABLE_NAME from INFORMATION_SCHEMA.Tables where table_schema='doc'");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by from INFORMATION_SCHEMA.Tables where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo("test");
        assertThat(response.rows()[0][1]).isEqualTo(5);
        assertThat(response.rows()[0][2]).isEqualTo("0-1");
        assertThat(response.rows()[0][3]).isEqualTo("col1");
    }

    @Test
    public void testSelectBlobTablesFromInformationSchemaTable() {
        execute("select TABLE_NAME from INFORMATION_SCHEMA.Tables where table_schema='blob'");
        assertThat(response.rowCount()).isEqualTo(0L);

        String blobsPath = createTempDir().toAbsolutePath().toString();
        execute("create blob table test clustered into 5 shards with (blobs_path=?)", new Object[]{blobsPath});
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by, blobs_path, routing_hash_function, version " +
                "from INFORMATION_SCHEMA.Tables where table_schema='blob' ");
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo("test");
        assertThat(response.rows()[0][1]).isEqualTo(5);
        assertThat(response.rows()[0][2]).isEqualTo("0-1");
        assertThat(response.rows()[0][3]).isEqualTo("digest");
        assertThat(response.rows()[0][4]).isEqualTo(blobsPath);
        assertThat(response.rows()[0][5]).isEqualTo(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME);
        TestingHelpers.assertCrateVersion(response.rows()[0][6], Version.CURRENT, null);

        // cleanup blobs path, tempDir hook will be deleted before table would be deleted, avoid error in log
        execute("drop blob table test");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSelectPartitionedTablesFromInformationSchemaTable() {
        execute("create table test (id int, name string, o object as (i int), primary key (id, o['i']))" +
                " partitioned by (o['i'])");
        execute("insert into test (id, name, o) values (1, 'Youri', {i=10}), (2, 'Ruben', {i=20})");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by, partitioned_by from INFORMATION_SCHEMA.Tables where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo("test");
        assertThat(response.rows()[0][1]).isEqualTo(4);
        assertThat(response.rows()[0][2]).isEqualTo("0-1");
        assertThat(response.rows()[0][3]).isEqualTo("_id");
        assertThat((List<Object>) response.rows()[0][4]).containsExactly("o['i']");
    }

    @Test
    public void testSelectStarFromInformationSchemaTable() {
        execute("create table test (col1 integer, col2 string) clustered into 5 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(1L);

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[0][13]).isEqualTo(sqlExecutor.getCurrentSchema());
        assertThat(response.rows()[0][12]).isEqualTo("test");
        assertThat(response.rows()[0][11]).isEqualTo("crate");
        assertThat(response.rows()[0][14]).isEqualTo("BASE TABLE");
        assertThat(response.rows()[0][8]).isEqualTo(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME);
        assertThat(response.rows()[0][5]).isEqualTo(5);
        assertThat(response.rows()[0][4]).isEqualTo("0-1");
        assertThat(response.rows()[0][2]).isEqualTo("_id");
        assertThat(response.rows()[0][1]).isEqualTo(false);
    }

    @Test
    public void testSelectFromTableConstraints() throws Exception {
        execute("SELECT column_name FROM information_schema.columns WHERE table_schema='information_schema' " +
                "AND table_name='table_constraints'");
        assertThat(response.rowCount()).isEqualTo(9L);
        assertThat(TestingHelpers.getColumn(response.rows(),0)).containsExactly(
            "constraint_catalog",
            "constraint_name",
            "constraint_schema",
            "constraint_type",
            "initially_deferred",
            "is_deferrable",
            "table_catalog",
            "table_name",
            "table_schema");
        execute("SELECT constraint_name, constraint_type, table_name, table_schema FROM " +
                "information_schema.table_constraints ORDER BY table_schema ASC, table_name ASC");
        assertThat(response).hasRows(
            "columns_pk| PRIMARY KEY| columns| information_schema",
            "information_schema_columns_column_name_not_null| CHECK| columns| information_schema",
            "information_schema_columns_data_type_not_null| CHECK| columns| information_schema",
            "information_schema_columns_is_generated_not_null| CHECK| columns| information_schema",
            "information_schema_columns_is_nullable_not_null| CHECK| columns| information_schema",
            "information_schema_columns_ordinal_position_not_null| CHECK| columns| information_schema",
            "information_schema_columns_table_catalog_not_null| CHECK| columns| information_schema",
            "information_schema_columns_table_name_not_null| CHECK| columns| information_schema",
            "information_schema_columns_table_schema_not_null| CHECK| columns| information_schema",
            "key_column_usage_pk| PRIMARY KEY| key_column_usage| information_schema",
            "referential_constraints_pk| PRIMARY KEY| referential_constraints| information_schema",
            "schemata_pk| PRIMARY KEY| schemata| information_schema",
            "sql_features_pk| PRIMARY KEY| sql_features| information_schema",
            "table_constraints_pk| PRIMARY KEY| table_constraints| information_schema",
            "table_partitions_pk| PRIMARY KEY| table_partitions| information_schema",
            "tables_pk| PRIMARY KEY| tables| information_schema",
            "views_pk| PRIMARY KEY| views| information_schema",
            "allocations_pk| PRIMARY KEY| allocations| sys",
            "checks_pk| PRIMARY KEY| checks| sys",
            "jobs_pk| PRIMARY KEY| jobs| sys",
            "jobs_log_pk| PRIMARY KEY| jobs_log| sys",
            "node_checks_pk| PRIMARY KEY| node_checks| sys",
            "nodes_pk| PRIMARY KEY| nodes| sys",
            "privileges_pk| PRIMARY KEY| privileges| sys",
            "repositories_pk| PRIMARY KEY| repositories| sys",
            "roles_pk| PRIMARY KEY| roles| sys",
            "shards_pk| PRIMARY KEY| shards| sys",
            "snapshot_restore_pk| PRIMARY KEY| snapshot_restore| sys",
            "snapshots_pk| PRIMARY KEY| snapshots| sys",
            "summits_pk| PRIMARY KEY| summits| sys",
            "users_pk| PRIMARY KEY| users| sys"
        );

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
        assertThat(response.rowCount()).isEqualTo(6L); // 4 explicit constraints + 2 NOT NULL derived from composite PK
        assertThat(response.rows()[0][0]).isEqualTo("PRIMARY KEY");
        assertThat(response.rows()[0][1]).isEqualTo("test_pk");
        assertThat(response.rows()[0][2]).isEqualTo("test");
        assertThat(response.rows()[1][0]).isEqualTo("CHECK");
        assertThat((String) response.rows()[1][1]).isEqualTo(sqlExecutor.getCurrentSchema() + "_test_col1_not_null");
        assertThat(response.rows()[1][2]).isEqualTo("test");
        assertThat(response.rows()[2][0]).isEqualTo("CHECK");
        assertThat((String) response.rows()[2][1]).isEqualTo(sqlExecutor.getCurrentSchema() + "_test_col2_not_null");
        assertThat(response.rows()[2][2]).isEqualTo("test");
        assertThat(response.rows()[3][0]).isEqualTo("CHECK");
        assertThat(response.rows()[3][1]).isEqualTo(sqlExecutor.getCurrentSchema() + "_test_col3_not_null");
        assertThat(response.rows()[3][2]).isEqualTo("test");
        assertThat(response.rows()[4][0]).isEqualTo("CHECK");
        assertThat(response.rows()[4][1]).isEqualTo("chk_1");
        assertThat(response.rows()[4][2]).isEqualTo("test");
        assertThat(response.rows()[5][0]).isEqualTo("CHECK");
        assertThat(response.rows()[5][1]).isEqualTo("unnecessary_check");
        assertThat(response.rows()[5][2]).isEqualTo("test");
    }

    @Test
    public void testRefreshTableConstraints() {
        execute("CREATE TABLE test (col1 INTEGER PRIMARY KEY, col2 STRING)");
        ensureGreen();
        execute("SELECT table_name, constraint_name FROM Information_schema" +
                ".table_constraints WHERE table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L); // 1 PK + 1 NOT NULL derived from PK
        assertThat(response.rows()[0][0]).isEqualTo("test");
        assertThat(response.rows()[0][1]).isEqualTo("test_pk");
        assertThat(response.rows()[1][0]).isEqualTo("test");
        assertThat((String) response.rows()[1][1]).isEqualTo(sqlExecutor.getCurrentSchema() + "_test_col1_not_null");

        execute("CREATE TABLE test2 (" +
            "   col1a STRING PRIMARY KEY," +
            "   \"Col2a\" TIMESTAMP WITH TIME ZONE NOT NULL)");
        ensureGreen();
        execute("SELECT table_name, constraint_name FROM information_schema.table_constraints WHERE table_schema = ? " +
                "ORDER BY table_name ASC",
            new Object[]{sqlExecutor.getCurrentSchema()});

        assertThat(response.rowCount()).isEqualTo(5L); // 2 PK + 1 explicit NOT NULL + 2 NOT NULL derived from PK-s.
        assertThat(response.rows()[2][0]).isEqualTo("test2");
        assertThat(response.rows()[2][1]).isEqualTo("test2_pk");
        assertThat(response.rows()[3][0]).isEqualTo("test2");
        assertThat((String) response.rows()[3][1]).isEqualTo(sqlExecutor.getCurrentSchema() + "_test2_col1a_not_null");
        assertThat(response.rows()[4][0]).isEqualTo("test2");
        assertThat(response.rows()[4][1]).isEqualTo(sqlExecutor.getCurrentSchema() + "_test2_Col2a_not_null");
    }


    @Test
    public void testSelectAnalyzersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='ANALYZER' order by " +
                "routine_name desc limit 5");
        assertThat(response).hasRows(
            "whitespace",
            "stop",
            "standard",
            "simple",
            "keyword"
        );
    }

    @Test
    public void testSelectTokenizersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='TOKENIZER' order by " +
                "routine_name asc limit 5");
        assertThat(response).hasRows("standard");
    }

    @Test
    public void testSelectTokenFiltersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='TOKEN_FILTER' order by " +
                "routine_name asc limit 5");
        assertThat(response).hasRows(
            "cjk_bigram",
            "cjk_width",
            "dictionary_decompounder",
            "hunspell",
            "hyphenation_decompounder"
        );
    }

    @Test
    public void testSelectCharFiltersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='CHAR_FILTER' order by " +
                "routine_name asc");
        assertThat(response).hasRows(
            "mapping",
            "pattern_replace"
        );
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
        assertThat(response.rowCount()).isEqualTo(6L); // 3 PK + 3 NOT NULL derived from PK
        // Aligned with PG, every PK column produces a new NOT NULL row in table_constraints.
        assertThat(response).hasRows(
            "abc",
            "abc",
            "test1",
            "test1",
            "test2",
            "test2"
        );
    }

    @Test
    public void testDefaultColumns() {
        execute("select * from information_schema.columns order by table_schema, table_name");
        assertThat(response.rowCount()).isEqualTo(1009L);
    }

    @Test
    public void testColumnsColumns() {
        execute("select column_name, data_type from information_schema.columns where table_schema='information_schema' and table_name='columns' order by column_name asc");
        assertThat(response.rowCount()).isEqualTo(42L);
        assertThat(response).hasRows(
            "character_maximum_length| integer",
            "character_octet_length| integer",
            "character_set_catalog| text",
            "character_set_name| text",
            "character_set_schema| text",
            "check_action| integer",
            "check_references| text",
            "collation_catalog| text",
            "collation_name| text",
            "collation_schema| text",
            "column_default| text",
            "column_details| object",
            "column_details['name']| text",
            "column_details['path']| text_array",
            "column_name| text",
            "data_type| text",
            "datetime_precision| integer",
            "domain_catalog| text",
            "domain_name| text",
            "domain_schema| text",
            "generation_expression| text",
            "identity_cycle| boolean",
            "identity_generation| text",
            "identity_increment| text",
            "identity_maximum| text",
            "identity_minimum| text",
            "identity_start| text",
            "interval_precision| integer",
            "interval_type| text",
            "is_generated| text",
            "is_identity| boolean",
            "is_nullable| boolean",
            "numeric_precision| integer",
            "numeric_precision_radix| integer",
            "numeric_scale| integer",
            "ordinal_position| integer",
            "table_catalog| text",
            "table_name| text",
            "table_schema| text",
            "udt_catalog| text",
            "udt_name| text",
            "udt_schema| text"
        );
    }

    @Test
    public void testSupportedPgTypeColumns() {
        execute("select column_name, data_type from information_schema.columns " +
                "where table_schema = 'pg_catalog' " +
                "and table_name = 'pg_type' " +
                "order by 1 asc");
        assertThat(response).hasRows(
            "oid| integer",
            "typacl| text",
            "typalign| text",
            "typanalyze| regproc",
            "typarray| integer",
            "typbasetype| integer",
            "typbyval| boolean",
            "typcategory| text",
            "typcollation| integer",
            "typdefault| text",
            "typdefaultbin| text",
            "typdelim| text",
            "typelem| integer",
            "typinput| regproc",
            "typisdefined| boolean",
            "typlen| smallint",
            "typmodin| regproc",
            "typmodout| regproc",
            "typname| text",
            "typnamespace| integer",
            "typndims| integer",
            "typnotnull| boolean",
            "typoutput| regproc",
            "typowner| integer",
            "typreceive| regproc",
            "typrelid| integer",
            "typsend| regproc",
            "typstorage| text",
            "typsubscript| regproc",
            "typtype| text",
            "typtypmod| integer"
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
        assertThat(response).hasColumns(
            "character_maximum_length",
            "character_octet_length",
            "character_set_catalog",
            "character_set_name",
            "character_set_schema",
            "check_action",
            "check_references",
            "collation_catalog",
            "collation_name",
            "collation_schema",
            "column_default",
            "column_details",
            "column_name",
            "data_type",
            "datetime_precision",
            "domain_catalog",
            "domain_name",
            "domain_schema",
            "generation_expression",
            "identity_cycle",
            "identity_generation",
            "identity_increment",
            "identity_maximum",
            "identity_minimum",
            "identity_start",
            "interval_precision",
            "interval_type",
            "is_generated",
            "is_identity",
            "is_nullable",
            "numeric_precision",
            "numeric_precision_radix",
            "numeric_scale",
            "ordinal_position",
            "table_catalog",
            "table_name",
            "table_schema",
            "udt_catalog",
            "udt_name",
            "udt_schema"
        );

        assertThat(response.rowCount()).isEqualTo(11L);

        Map<String, Integer> cols = IntStream.range(0, response.cols().length)
                                             .boxed()
                                             .collect(Collectors.toMap(i -> response.cols()[i], i -> i));

        assertThat(response.rows()[0][cols.get("column_name")]).isEqualTo("age");
        assertThat(response.rows()[1][cols.get("column_name")]).isEqualTo("b");
        assertThat(response.rows()[2][cols.get("column_name")]).isEqualTo("col1");
        assertThat(response.rows()[3][cols.get("column_name")]).isEqualTo("col2");
        assertThat(response.rows()[4][cols.get("column_name")]).isEqualTo("d");
        assertThat(response.rows()[5][cols.get("column_name")]).isEqualTo("f");
        assertThat(response.rows()[6][cols.get("column_name")]).isEqualTo("s");
        assertThat(response.rows()[7][cols.get("column_name")]).isEqualTo("stuff");
        assertThat(response.rows()[8][cols.get("column_name")]).isEqualTo("stuff['level1']");
        assertThat(response.rows()[9][cols.get("column_name")]).isEqualTo("stuff['level1']['level2']");
        assertThat(response.rows()[10][cols.get("column_name")]).isEqualTo("stuff['level1']['level2_nullable']");

        assertThat(response.rows()[0][cols.get("data_type")]).isEqualTo("integer");
        assertThat(response.rows()[0][cols.get("datetime_precision")]).isEqualTo(null);
        assertThat(response.rows()[0][cols.get("is_generated")]).isEqualTo("NEVER");
        assertThat(response.rows()[0][cols.get("numeric_precision")]).isEqualTo(32);
        assertThat(response.rows()[0][cols.get("numeric_precision_radix")]).isEqualTo(2);
        assertThat(response.rows()[0][cols.get("ordinal_position")]).isEqualTo(3);
        assertThat(response.rows()[0][cols.get("table_catalog")]).isEqualTo("crate");
        assertThat(response.rows()[0][cols.get("table_name")]).isEqualTo("test");

        assertThat(response.rows()[0][cols.get("is_identity")]).isEqualTo(false);
        assertThat(response.rows()[0][cols.get("identity_generation")]).isEqualTo(null);
        assertThat(response.rows()[0][cols.get("identity_start")]).isEqualTo(null);
        assertThat(response.rows()[0][cols.get("identity_increment")]).isEqualTo(null);
        assertThat(response.rows()[0][cols.get("identity_maximum")]).isEqualTo(null);
        assertThat(response.rows()[0][cols.get("identity_minimum")]).isEqualTo(null);
        assertThat(response.rows()[0][cols.get("identity_cycle")]).isEqualTo(null);

        assertThat(response.rows()[0][cols.get("is_nullable")]).isEqualTo(false);
        assertThat(response.rows()[2][cols.get("is_nullable")]).isEqualTo(false);
        assertThat(response.rows()[3][cols.get("is_nullable")]).isEqualTo(true);
        assertThat(response.rows()[7][cols.get("is_nullable")]).isEqualTo(false);
        assertThat(response.rows()[8][cols.get("is_nullable")]).isEqualTo(false);
        assertThat(response.rows()[9][cols.get("is_nullable")]).isEqualTo(false);
        assertThat(response.rows()[10][cols.get("is_nullable")]).isEqualTo(true);

        assertThat(response.rows()[1][cols.get("numeric_precision")]).isEqualTo(8);
        assertThat(response.rows()[6][cols.get("numeric_precision")]).isEqualTo(16);
        assertThat(response.rows()[4][cols.get("numeric_precision")]).isEqualTo(53);
        assertThat(response.rows()[5][cols.get("numeric_precision")]).isEqualTo(24);

        assertThat(response.rows()[7][cols.get("column_details")]).isEqualTo(Map.of("name","stuff","path", List.of())); // column_details
        assertThat(response.rows()[8][cols.get("column_details")]).isEqualTo(Map.of("name","stuff","path", List.of("level1"))); // column_details
        assertThat(response.rows()[9][cols.get("column_details")]).isEqualTo(Map.of("name","stuff","path", List.of("level1","level2"))); // column_details
        assertThat(response.rows()[10][cols.get("column_details")]).isEqualTo(Map.of("name","stuff","path", List.of("level1","level2_nullable"))); // column_details
    }

    @Test
    public void testSelectFromTableColumnsRefresh() {
        execute("create table test (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response.rows()[0][0]).isEqualTo("test");

        execute("create table test2 (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns " +
                "where table_schema = ? " +
                "order by table_name", new Object[]{sqlExecutor.getCurrentSchema()});

        assertThat(response.rowCount()).isEqualTo(6L);
        assertThat(response.rows()[0][0]).isEqualTo("test");
        assertThat(response.rows()[4][0]).isEqualTo("test2");
    }

    @Test
    public void testSelectFromTableColumnsMultiField() {
        execute("create table test (col1 string, col2 string," +
                "index col1_col2_ft using fulltext(col1, col2))");
        ensureGreen();
        execute("select table_name, column_name," +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L);

        assertThat(response.rows()[0][0]).isEqualTo("test");
        assertThat(response.rows()[0][1]).isEqualTo("col1");
        assertThat(response.rows()[0][2]).isEqualTo(1);
        assertThat(response.rows()[0][3]).isEqualTo("text");

        assertThat(response.rows()[1][0]).isEqualTo("test");
        assertThat(response.rows()[1][1]).isEqualTo("col2");
        assertThat(response.rows()[1][2]).isEqualTo(2);
        assertThat(response.rows()[1][3]).isEqualTo("text");
    }

    @Test
    public void testGlobalAggregation() {
        execute("select max(ordinal_position) from information_schema.columns");
        assertThat(response.rowCount()).isEqualTo(1);

        assertThat(response.rows()[0][0]).isEqualTo(122);

        execute("create table t1 (id integer, col1 string)");
        execute("select max(ordinal_position) from information_schema.columns where table_schema = ?",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(1);

        assertThat(response.rows()[0][0]).isEqualTo(2);
    }

    @Test
    public void testGlobalAggregationMany() {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=0)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=0)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=0)");
        ensureYellow();
        execute("select min(number_of_shards), max(number_of_shards), avg(number_of_shards)," +
                "sum(number_of_shards) from information_schema.tables where table_schema = ?", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(1);

        assertThat(response.rows()[0][0]).isEqualTo(3);
        assertThat(response.rows()[0][1]).isEqualTo(10);
        assertThat(response.rows()[0][2]).isEqualTo(6.0d);
        assertThat(response.rows()[0][3]).isEqualTo(18L);
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
        assertThat(response.rowCount()).isEqualTo(1);

        assertThat(response.rows()[0][0]).isEqualTo(2);
        assertThat(response.rows()[0][1]).isEqualTo(3);
        assertThat(response.rows()[0][2]).isEqualTo(2.5d);
        assertThat(response.rows()[0][3]).isEqualTo(5L);
    }

    @Test
    public void testGlobalAggregationWithAlias() {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=0)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=0)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=0)");
        ensureYellow();
        execute("select min(number_of_shards) as min_shards from information_schema.tables where table_name = 't1'");
        assertThat(response.rowCount()).isEqualTo(1);

        assertThat(response.rows()[0][0]).isEqualTo(10);
    }

    @Test
    public void testGlobalCount() {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=0)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=0)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=0)");
        execute("select count(*) from information_schema.tables");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(70L);
    }

    @Test
    public void testGlobalCountDistinct() {
        execute("create table t3 (TableInfoid integer, col1 string)");
        ensureGreen();
        execute("select count(distinct table_schema) from information_schema.tables order by count(distinct table_schema)");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(4L);
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
        assertThat(response.rowCount()).isEqualTo(4);

        execute("insert into t4 (stuff) values (?)", new Object[] {
            Map.of(
                "first_name", "Douglas",
                "middle_name", "Noel",
                "last_name", "Adams")});
        execute("refresh table t4");

        execute("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertThat(response.rowCount()).isEqualTo(5);
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
        assertThat(response.rowCount()).isEqualTo(4);

        execute("insert into t4 (stuff) values (?)", new Object[] {
            Map.of(
                "first_name", "Douglas",
                "middle_name", "Noel",
                "last_name", "Adams")});

        execute("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertThat(response.rowCount()).isEqualTo(4);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPartitionedBy() {
        execute("create table my_table (id integer, name string) partitioned by (name)");
        execute("create table my_other_table (id integer, name string, content string) " +
                "partitioned by (name, content)");

        execute("select * from information_schema.tables " +
                "where table_schema = ? order by table_name", new Object[]{sqlExecutor.getCurrentSchema()});

        assertThat((List<Object>) response.rows()[0][6]).containsExactly("name", "content");
        assertThat((List<Object>) response.rows()[1][6]).containsExactly("name");
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
        assertThat(response.rowCount()).isEqualTo(3);

        Object[] row1 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "04132", Map.of("par", 1), "1", 5, "0-1", "1"};
        Object[] row2 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "04134", Map.of("par", 2), "2", 5, "0-1", "1"};
        Object[] row3 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "04136", Map.of("par", 3), "3", 5, "0-1", "1"};

        assertThat(response.rows()[0]).containsExactly(row1);
        assertThat(response.rows()[1]).containsExactly(row2);
        assertThat(response.rows()[2]).containsExactly(row3);
    }

    @Test
    public void testPartitionedTableShardsAndReplicas() throws Exception {
        execute("create table parted (par byte, content string) " +
                "partitioned by (par) " +
                "clustered into 2 shards with (number_of_replicas=0, column_policy='dynamic')");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(response).hasRows("parted| 2| 0");

        execute("select * from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount()).isEqualTo(0L);

        execute("insert into parted (par, content) values (1, 'foo'), (3, 'baz')");
        execute("refresh table parted");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(response).hasRows("parted| 2| 0");

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response).hasRows(
            "parted| 04132| {par=1}| 2| 0",
            "parted| 04136| {par=3}| 2| 0");

        execute("alter table parted set (number_of_shards=6)");
        waitNoPendingTasksOnAll();

        execute("insert into parted (par, content) values (2, 'bar')");
        execute("refresh table parted");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(response).hasRows("parted| 6| 0");

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response).hasRows(
            "parted| 04132| {par=1}| 2| 0",
            "parted| 04134| {par=2}| 6| 0",
            "parted| 04136| {par=3}| 2| 0");

        execute("update parted set new=true where par=1");
        execute("refresh table parted");
        waitNoPendingTasksOnAll();

        // ensure newer index metadata does not override settings in template
        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(response).hasRows("parted| 6| 0");

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response).hasRows(
            "parted| 04132| {par=1}| 2| 0",
            "parted| 04134| {par=2}| 6| 0",
            "parted| 04136| {par=3}| 2| 0");

        execute("delete from parted where par=2");
        waitNoPendingTasksOnAll();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(response).hasRows("parted| 6| 0");
        waitNoPendingTasksOnAll();
        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response).hasRows(
            "parted| 04132| {par=1}| 2| 0",
            "parted| 04136| {par=3}| 2| 0");


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
        execute("refresh table my_table");
        execute("alter table my_table set (number_of_shards=4)");
        waitNoPendingTasksOnAll();
        execute("insert into my_table (par, par_str, content) values (2, 'asdf', 'content5')");
        ensureYellow();

        execute("select table_name, table_schema, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions order by table_name, partition_ident");
        assertThat(response.rowCount()).isEqualTo(5);

        Object[] row1 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "08132132c5p0", Map.of("par", 1, "par_str", "bar"), 5, "0-1"};
        Object[] row2 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "08132136dtng", Map.of("par", 1, "par_str", "foo"), 5, "0-1"};
        Object[] row3 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "08134132c5p0", Map.of("par", 2, "par_str", "bar"), 5, "0-1"};
        Object[] row4 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "08134136dtng", Map.of("par", 2, "par_str", "foo"), 5, "0-1"};
        Object[] row5 = new Object[]{"my_table", sqlExecutor.getCurrentSchema(), "081341b1edi6c", Map.of("par", 2, "par_str", "asdf"), 4, "0-1"};

        assertThat(response.rows()[0]).containsExactly(row1);
        assertThat(response.rows()[1]).containsExactly(row2);
        assertThat(response.rows()[2]).containsExactly(row3);
        assertThat(response.rows()[3]).containsExactly(row4);
        assertThat(response.rows()[4]).containsExactly(row5);
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
        execute("refresh table my_table");

        execute("select table_name, partition_ident, values from information_schema.table_partitions order by table_name, partition_ident");
        assertThat(response.rowCount()).isEqualTo(2);

        assertThat(response).hasRows(
            "my_table| 04130| {metadata['date']=0}",
            "my_table| 04732d1g64p36d9i60o30c1g| {metadata['date']=1401235200000}");
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
        assertThat(response.rowCount()).isEqualTo(1L);
        assertThat(response.rows()[0][0]).isEqualTo("any1");
    }

    @Test
    public void testDynamicObjectPartitionedTableInformationSchemaColumns() throws Exception {
        String stmtCreate = "create table data_points (" +
                            "day string primary key," +
                            "data object(dynamic)" +
                            ") partitioned by (day)";
        execute(stmtCreate);

        String stmtInsert = "insert into data_points (day, data) values (?, ?)";
        Map<String, Object> obj = Map.of(
            "somestringroute", "stringvalue",
            "somelongroute", 1338L);
        Object[] argsInsert = new Object[] {
            "20140520",
            obj
        };
        execute(stmtInsert, argsInsert);
        assertThat(response.rowCount()).isEqualTo(1L);
        execute("refresh table data_points");

        String stmtIsColumns = "select table_name, column_name, data_type " +
                               "from information_schema.columns " +
                               "where table_name = 'data_points' " +
                               "order by column_name";
        execute(stmtIsColumns);
        assertThat(response.rowCount()).isEqualTo(4L);

        assertThat(response).hasRows(
            "data_points| data| object",
            "data_points| data['somelongroute']| bigint",
            "data_points| data['somestringroute']| text",
            "data_points| day| text"
        );
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testRegexpMatch() {
        execute("create blob table blob_t1");
        execute("create table t(id String)");
        ensureYellow();
        execute("select distinct table_schema from information_schema.tables " +
                "where table_schema ~ '[a-z]+o[a-z]' order by table_schema");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(response.rows()[0][0]).isEqualTo("blob");
        assertThat(response.rows()[1][0]).isEqualTo("doc");
    }

    @Test
    @UseRandomizedSchema(random = false)
    public void testSelectSchemata() throws Exception {
        execute("select * from information_schema.schemata order by schema_name asc");
        assertThat(response.rowCount()).isEqualTo(5L);
        assertThat(TestingHelpers.getColumn(response.rows(), 0))
            .containsExactly("blob", "doc", "information_schema", "pg_catalog", "sys");

        execute("create table t1 (col string) with (number_of_replicas=0)");
        ensureGreen();

        execute("select * from information_schema.schemata order by schema_name asc");
        assertThat(response.rowCount()).isEqualTo(5L);
        assertThat(TestingHelpers.getColumn(response.rows(), 0))
            .containsExactly("blob", "doc", "information_schema", "pg_catalog", "sys");
    }

    @Test
    public void testSelectGeneratedColumnFromInformationSchemaColumns() {
        execute("create table t (lastname string, firstname string, name as (lastname || '_' || firstname)) " +
                "with (number_of_replicas = 0)");
        execute("select column_name, is_generated, generation_expression from information_schema.columns where is_generated = 'ALWAYS'");
        assertThat(response).hasRows(
            "name| ALWAYS| concat(concat(lastname, '_'), firstname)");
    }

    @Test
    public void testSelectSqlFeatures() {
        execute("select * from information_schema.sql_features order by feature_id asc");
        assertThat(response).hasRowCount(679L);

        execute("select feature_id, feature_name from information_schema.sql_features where feature_id='E011'");
        assertThat(response).hasRowCount(7L);
        assertThat(response.rows()[0][1]).isEqualTo("Numeric data types");
    }

    @Test
    public void testScalarEvaluatesInErrorOnInformationSchema() {
        Asserts.assertSQLError(() -> execute("select 1/0 from information_schema.tables"))
            .hasPGError(INTERNAL_ERROR)
            .hasHTTPError(BAD_REQUEST, 4000)
            .hasMessageContaining("/ by zero");
    }

    @Test
    public void testInformationRoutinesColumns() {
        execute("select column_name from information_schema.columns where table_name='routines' order by column_name asc");
        assertThat(response).hasRows(
            "data_type",
            "is_deterministic",
            "routine_body",
            "routine_definition",
            "routine_name",
            "routine_schema",
            "routine_type",
            "specific_name"
        );
    }

    @Test
    public void testOpenCloseTableInformation() {
        execute("create table t (i int)");
        ensureYellow();

        execute("alter table t close");
        execute("select closed from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(true);

        execute("alter table t open");
        execute("select closed from information_schema.tables where table_name = 't'");
        assertThat(response.rowCount()).isEqualTo(1);
        assertThat(response.rows()[0][0]).isEqualTo(false);
    }

    @Test
    public void testOpenClosePartitionInformation() {
        execute("create table t (i int) partitioned by (i)");

        execute("insert into t values (1)");
        String partitionIdent = new PartitionName(
            new RelationName(sqlExecutor.getCurrentSchema(), "t"),
            Collections.singletonList("1")
        ).ident();

        execute("insert into t values (2), (3)");
        ensureGreen();

        execute("select closed from information_schema.table_partitions where table_name = 't'");

        assertThat(response.rowCount()).isEqualTo(3);
        assertThat(response.rows()[0][0]).isEqualTo(false);
        assertThat(response.rows()[1][0]).isEqualTo(false);
        assertThat(response.rows()[2][0]).isEqualTo(false);

        execute("alter table t partition (i = 1) close");
        execute("select partition_ident, values from information_schema.table_partitions" +
                " where table_name = 't' and closed = true");

        assertThat(response.rowCount()).isEqualTo(1);

        Map<?, ?> values = (Map<?, ?>) response.rows()[0][1];
        assertThat(values.get("i")).isEqualTo(1);
        assertThat(partitionIdent).endsWith((String) response.rows()[0][0]);

        execute("alter table t partition (i = 1) open");
        execute("select closed from information_schema.table_partitions");

        assertThat(response.rowCount()).isEqualTo(3);
        assertThat(response.rows()[0][0]).isEqualTo(false);
        assertThat(response.rows()[1][0]).isEqualTo(false);
        assertThat(response.rows()[2][0]).isEqualTo(false);
    }

    @Test
    public void testSelectFromKeyColumnUsage() {
        execute("create table table1 (id1 integer)");
        execute("create table table2 (id2 integer primary key)");
        execute("create table table3 (id3 integer, name string, other double, primary key (id3, name))");
        ensureYellow();

        execute("select * from information_schema.key_column_usage order by table_name, ordinal_position asc");
        assertThat(response.rowCount()).isEqualTo(3L);
        assertThat(response).hasColumns(
            "column_name",
            "constraint_catalog",
            "constraint_name",
            "constraint_schema",
            "ordinal_position",
            "table_catalog",
            "table_name",
            "table_schema"
        );

        final String defaultSchema = sqlExecutor.getCurrentSchema();
        Object[][] expectedRows = new Object[][] {
            new Object[]{"id2", "crate", "table2_pk", defaultSchema, 1, "crate", "table2", defaultSchema},
            new Object[]{"id3", "crate", "table3_pk", defaultSchema, 1, "crate", "table3", defaultSchema},
            new Object[]{"name", "crate", "table3_pk", defaultSchema, 2, "crate", "table3", defaultSchema}
        };
        assertThat(response.rows()).isEqualTo(expectedRows);

        // check that the constraint name is the same as in table_constraints by joining the two tables
        execute("select t1.* from information_schema.key_column_usage t1 " +
                "join information_schema.table_constraints t2 " +
                "on t1.constraint_name = t2.constraint_name " +
                "order by t1.table_name, t1.ordinal_position asc");
        assertThat(response.rows()).isEqualTo(expectedRows);
    }

    @Test
    public void testSelectFromReferentialConstraints() {
        execute("select * from information_schema.referential_constraints");
        assertThat(response.rowCount()).isEqualTo(0L);
        assertThat(response).hasColumns(
            "constraint_catalog",
            "constraint_name",
            "constraint_schema",
            "delete_rule",
            "match_option",
            "unique_constraint_catalog",
            "unique_constraint_name",
            "unique_constraint_schema",
            "update_rule"
        );
    }

    @Test
    public void test_character_maximum_length_information_schema_columns() {
        execute("CREATE TABLE t (col1 varchar, col2 varchar(1)) CLUSTERED INTO 1 SHARDS");
        execute("SELECT column_name, character_maximum_length " +
                "FROM information_schema.columns " +
                "WHERE table_name = 't'");
        assertThat(response).hasRows(
            "col1| NULL",
            "col2| 1");
    }

    @Test
    public void test_dropped_columns_are_not_shown_in_information_schema() {
        execute("create table t(a integer, o object AS(oo object AS(a int)))");

        execute("alter table t drop column a");
        execute("alter table t drop column o['oo']"); // Implicitly drops children.

        // Only object column's root is left.
        execute("""
            select column_name, ordinal_position
            from information_schema.columns
            where table_name = 't'
            order by ordinal_position"""
        );
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("o| 2\n");
    }

    @Test
    public void test_primary_key_constraint_names_are_visible() {
        execute("create table t (a int constraint c_1 primary key, b int constraint c_1 primary key)");
        execute("""
            select distinct constraint_name
            from information_schema.table_constraints
            where table_name = 't' and constraint_type = 'PRIMARY KEY'
            """);
        assertThat(response).hasRows("c_1");
    }
}
