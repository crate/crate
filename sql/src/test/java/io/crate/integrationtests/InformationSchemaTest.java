/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.crate.Version;
import io.crate.action.sql.SQLActionException;
import io.crate.metadata.IndexMappings;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseRandomizedSchema;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

@ESIntegTestCase.ClusterScope(numDataNodes = 2)
public class InformationSchemaTest extends SQLTransportIntegrationTest {

    @Test
    public void testDefaultTables() {
        execute("select * from information_schema.tables order by table_schema, table_name");
        assertEquals(25L, response.rowCount());

        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| columns| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| ingestion_rules| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| key_column_usage| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| referential_constraints| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| routines| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| schemata| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| sql_features| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| table_constraints| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| table_partitions| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| information_schema| tables| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| pg_catalog| pg_type| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| allocations| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| checks| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| cluster| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| health| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| jobs| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| jobs_log| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| node_checks| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| nodes| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| operations| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| operations_log| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| repositories| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| shards| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| snapshots| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| NULL| NULL| NULL| SYSTEM GENERATED| NULL| NULL| NULL| sys| summits| sys| BASE TABLE| NULL\n")
        );
    }

    @Test
    public void testSelectFromInformationSchema() {
        execute("create table quotes (" +
                "id integer primary key, " +
                "quote string index off, " +
                "__quote_info int, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='snowball')" +
                ") clustered by (id) into 3 shards with (number_of_replicas=0)");

        execute("select table_name, number_of_shards, number_of_replicas, clustered_by from " +
                "information_schema.tables " +
                "where table_name='quotes'");
        assertEquals(1L, response.rowCount());
        assertEquals("quotes", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals("0", response.rows()[0][2]);
        assertEquals("id", response.rows()[0][3]);

        execute("select * from information_schema.columns where table_name='quotes'");
        assertEquals(3L, response.rowCount());

        execute("select * from information_schema.table_constraints where table_schema = ? and table_name='quotes'",
            new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(1L, response.rowCount());

        execute("select table_name from information_schema.columns where table_schema = ? and table_name='quotes' " +
                "and column_name='__quote_info'", new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(1L, response.rowCount());

        execute("select * from information_schema.routines");
        assertEquals(125L, response.rowCount());
    }

    @Test
    public void testSearchInformationSchemaTablesRefresh() {
        execute("select * from information_schema.tables");
        assertEquals(25L, response.rowCount());

        execute("create table t4 (col1 integer, col2 string) with(number_of_replicas=0)");
        ensureYellow(getFqn("t4"));

        execute("select * from information_schema.tables");
        assertEquals(26L, response.rowCount());
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderBy() {
        String defaultSchema = sqlExecutor.getDefaultSchema();
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
                "order by table_name asc limit 1", new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(1L, response.rowCount());
        assertEquals(sqlExecutor.getDefaultSchema(), response.rows()[0][0]);
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
                "order by number_of_shards desc, table_name asc limit 2", new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(2L, response.rowCount());

        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "bar| 3\n" +
            "foo| 3\n"));
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimitOffset() {
        String defaultSchema = sqlExecutor.getDefaultSchema();
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
            new Object[]{sqlExecutor.getDefaultSchema()});
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
        execute("create table test (id int primary key, name string) partitioned by (id)");
        execute("insert into test (id, name) values (1, 'Youri'), (2, 'Ruben')");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by, partitioned_by from INFORMATION_SCHEMA.Tables where table_schema = ?",
            new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(4, response.rows()[0][1]);
        assertEquals("0-1", response.rows()[0][2]);
        assertEquals("id", response.rows()[0][3]);
        assertThat((Object[]) response.rows()[0][4], arrayContaining(new Object[]{"id"}));
    }

    @Test
    public void testSelectStarFromInformationSchemaTable() {
        execute("create table test (col1 integer, col2 string) clustered into 5 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema = ?", new Object[]{sqlExecutor.getDefaultSchema()});
        assertThat(response.rowCount(), is(1L));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[0][13], is(sqlExecutor.getDefaultSchema()));
        assertThat(response.rows()[0][12], is("test"));
        assertThat(response.rows()[0][11], is(sqlExecutor.getDefaultSchema()));
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
        assertEquals(23L, response.rowCount());
        assertThat(TestingHelpers.printedTable(response.rows()),
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
                "ingestion_rules_pk| PRIMARY KEY| ingestion_rules| information_schema\n" +
                "schemata_pk| PRIMARY KEY| schemata| information_schema\n" +
                "sql_features_pk| PRIMARY KEY| sql_features| information_schema\n" +
                "tables_pk| PRIMARY KEY| tables| information_schema\n" +
                "allocations_pk| PRIMARY KEY| allocations| sys\n" +
                "checks_pk| PRIMARY KEY| checks| sys\n" +
                "jobs_pk| PRIMARY KEY| jobs| sys\n" +
                "jobs_log_pk| PRIMARY KEY| jobs_log| sys\n" +
                "node_checks_pk| PRIMARY KEY| node_checks| sys\n" +
                "nodes_pk| PRIMARY KEY| nodes| sys\n" +
                "repositories_pk| PRIMARY KEY| repositories| sys\n" +
                "shards_pk| PRIMARY KEY| shards| sys\n" +
                "snapshots_pk| PRIMARY KEY| snapshots| sys\n" +
                "summits_pk| PRIMARY KEY| summits| sys\n"
            ));

        execute("CREATE TABLE test (col1 INTEGER, col2 INTEGER, col3 INT NOT NULL, col4 STRING, " +
                "PRIMARY KEY(col1,col2))");
        ensureGreen();
        execute("SELECT constraint_type, constraint_name, table_name FROM information_schema.table_constraints " +
                "WHERE table_schema = ?",
            new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(2L, response.rowCount());
        assertThat(response.rows()[0][0], is("PRIMARY KEY"));
        assertThat(response.rows()[0][1], is("test_pk"));
        assertThat(response.rows()[0][2], is("test"));
        assertThat(response.rows()[1][0], is("CHECK"));
        assertThat(response.rows()[1][1], is(sqlExecutor.getDefaultSchema() + "_test_col3_not_null"));
        assertThat(response.rows()[1][2], is("test"));
    }

    @Test
    public void testRefreshTableConstraints() {
        execute("CREATE TABLE test (col1 INTEGER PRIMARY KEY, col2 STRING)");
        ensureGreen();
        execute("SELECT table_name, constraint_name FROM Information_schema" +
                ".table_constraints WHERE table_schema = ?", new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(1L, response.rowCount());
        assertThat(response.rows()[0][0], is("test"));
        assertThat(response.rows()[0][1], is("test_pk"));

        execute("CREATE TABLE test2 (col1a STRING PRIMARY KEY, \"Col2a\" TIMESTAMP NOT NULL)");
        ensureGreen();
        execute("SELECT table_name, constraint_name FROM information_schema.table_constraints WHERE table_schema = ? " +
                "ORDER BY table_name ASC",
            new Object[]{sqlExecutor.getDefaultSchema()});

        assertEquals(3L, response.rowCount());
        assertThat(response.rows()[1][0], is("test2"));
        assertThat(response.rows()[1][1], is("test2_pk"));
        assertThat(response.rows()[2][0], is("test2"));
        assertThat(response.rows()[2][1], is(sqlExecutor.getDefaultSchema() + "_test2_Col2a_not_null"));
    }

    @Test
    public void testSelectFromRoutines() {
        String stmt1 = "CREATE ANALYZER myAnalyzer WITH (" +
                       "  TOKENIZER whitespace," +
                       "  TOKEN_FILTERS (" +
                       "     myTokenFilter WITH (" +
                       "      type='snowball'," +
                       "      language='german'" +
                       "    )," +
                       "    kstem" +
                       "  )" +
                       ")";
        execute(stmt1);
        execute("CREATE ANALYZER myOtherAnalyzer extends german (" +
                "  stopwords=[?, ?, ?]" +
                ")", new Object[]{"der", "die", "das"});
        ensureGreen();
        execute("SELECT routine_name, routine_type from INFORMATION_SCHEMA.routines " +
                "where routine_name = 'myanalyzer' " +
                "or routine_name = 'myotheranalyzer' " +
                "and routine_type = 'ANALYZER' " +
                "order by routine_name asc");
        assertEquals(2L, response.rowCount());

        assertEquals("myanalyzer", response.rows()[0][0]);
        assertEquals("ANALYZER", response.rows()[0][1]);
        assertEquals("myotheranalyzer", response.rows()[1][0]);
        assertEquals("ANALYZER", response.rows()[1][1]);
        client().admin().cluster().prepareUpdateSettings()
            .setPersistentSettings(
                MapBuilder.<String, Object>newMapBuilder()
                    .put("crate.analysis.custom.analyzer.myanalyzer", null)
                    .put("crate.analysis.custom.analyzer.myotheranalyzer", null)
                    .put("crate.analysis.custom.filter.myanalyzer_mytokenfilter", null)
                    .map())
            .setTransientSettings(
                MapBuilder.<String, Object>newMapBuilder()
                    .put("crate.analysis.custom.analyzer.myanalyzer", null)
                    .put("crate.analysis.custom.analyzer.myotheranalyzer", null)
                    .put("crate.analysis.custom.filter.myanalyzer_mytokenfilter", null)
                    .map())
            .execute().actionGet();
    }

    @Test
    public void testSelectAnalyzersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='ANALYZER' order by " +
                "routine_name desc limit 5");
        assertEquals(5L, response.rowCount());
        String[] analyzerNames = new String[response.rows().length];
        for (int i = 0; i < response.rowCount(); i++) {
            analyzerNames[i] = (String) response.rows()[i][0];
        }
        assertEquals(
            "whitespace, turkish, thai, swedish, stop",
            Joiner.on(", ").join(analyzerNames)
        );
    }

    @Test
    public void testSelectTokenizersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='TOKENIZER' order by " +
                "routine_name asc limit 5");
        assertEquals(5L, response.rowCount());
        String[] tokenizerNames = new String[response.rows().length];
        for (int i = 0; i < response.rowCount(); i++) {
            tokenizerNames[i] = (String) response.rows()[i][0];
        }
        assertEquals(
            "PathHierarchy, classic, edgeNGram, edge_ngram, keyword",
            Joiner.on(", ").join(tokenizerNames)
        );
    }

    @Test
    public void testSelectTokenFiltersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='TOKEN_FILTER' order by " +
                "routine_name asc limit 5");
        assertEquals(5L, response.rowCount());
        String[] tokenFilterNames = new String[response.rows().length];
        for (int i = 0; i < response.rowCount(); i++) {
            tokenFilterNames[i] = (String) response.rows()[i][0];
        }
        assertEquals(
            "apostrophe, arabic_normalization, arabic_stem, asciifolding, bengali_normalization",
            Joiner.on(", ").join(tokenFilterNames)
        );
    }

    @Test
    public void testSelectCharFiltersFromRoutines() {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='CHAR_FILTER' order by " +
                "routine_name asc");
        assertEquals(3L, response.rowCount());
        String[] charFilterNames = new String[response.rows().length];
        for (int i = 0; i < response.rowCount(); i++) {
            charFilterNames[i] = (String) response.rows()[i][0];
        }
        assertEquals(
            "html_strip, mapping, pattern_replace",
            Joiner.on(", ").join(charFilterNames)
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
        assertEquals(3L, response.rowCount());
        assertEquals("abc", response.rows()[0][0]);
        assertEquals("test1", response.rows()[1][0]);
        assertEquals("test2", response.rows()[2][0]);
    }

    @Test
    public void testDefaultColumns() {
        execute("select * from information_schema.columns order by table_schema, table_name");
        assertEquals(486, response.rowCount());
    }

    @Test
    public void testColumnsColumns() {
        execute("select * from information_schema.columns where table_schema='information_schema' and table_name='columns' order by ordinal_position asc");
        assertThat(response.rowCount(), is(32L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_maximum_length| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 32| 2| NULL| 1| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_octet_length| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 32| 2| NULL| 2| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_set_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 3| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_set_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 4| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_set_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 5| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| check_action| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 32| 2| NULL| 6| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| check_references| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 7| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| collation_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 8| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| collation_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 9| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| collation_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 10| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| column_default| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 11| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| column_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 12| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| data_type| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 13| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| datetime_precision| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 32| 2| NULL| 14| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| domain_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 15| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| domain_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 16| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| domain_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 17| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| generation_expression| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 18| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| interval_precision| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 32| 2| NULL| 19| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| interval_type| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 20| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| is_generated| boolean| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 21| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| is_nullable| boolean| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 22| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| numeric_precision| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 32| 2| NULL| 23| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| numeric_precision_radix| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 32| 2| NULL| 24| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| numeric_scale| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 32| 2| NULL| 25| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| ordinal_position| short| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| 16| 2| NULL| 26| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| table_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 27| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| table_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 28| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| table_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 29| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| user_defined_type_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 30| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| user_defined_type_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 31| information_schema| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| user_defined_type_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 32| information_schema| columns| information_schema| NULL| NULL| NULL\n")
        );
    }

    @Test
    public void testSelectFromTableColumns() {
        String defaultSchema = sqlExecutor.getDefaultSchema();
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
        execute("select * from INFORMATION_SCHEMA.Columns where table_schema = ?", new Object[]{defaultSchema});
        assertEquals(11L, response.rowCount());
        assertEquals("age", response.rows()[0][11]); // column_name
        assertEquals("integer", response.rows()[0][12]); // data_type
        assertEquals(null, response.rows()[0][13]); // datetime_precision
        assertEquals(false, response.rows()[0][20]); // is_generated
        assertEquals(false, response.rows()[0][21]); // is_nullable
        assertEquals(32, response.rows()[0][22]); // numeric_precision
        assertEquals(2, response.rows()[0][23]); // numeric_precision_radix

        assertEquals((short) 1, response.rows()[0][25]); // ordinal_position
        assertEquals(defaultSchema, response.rows()[0][26]); // table_catalog
        assertEquals("test", response.rows()[0][27]); // table_name
        assertEquals(defaultSchema, response.rows()[0][28]); // table_schema

        assertEquals("col1", response.rows()[2][11]);
        assertEquals(false, response.rows()[2][21]);

        assertEquals("col2", response.rows()[3][11]);
        assertEquals(true, response.rows()[3][21]);

        assertEquals("b", response.rows()[1][11]);
        assertEquals(8, response.rows()[1][22]);
        assertEquals("s", response.rows()[6][11]);
        assertEquals(16, response.rows()[6][22]);
        assertEquals("d", response.rows()[4][11]);
        assertEquals(53, response.rows()[4][22]);
        assertEquals("f", response.rows()[5][11]);
        assertEquals(24, response.rows()[5][22]);
        assertEquals("stuff", response.rows()[7][11]);
        assertEquals(false, response.rows()[7][21]); // is_nullable
        assertEquals("stuff['level1']", response.rows()[8][11]);
        assertEquals(false, response.rows()[8][21]); // is_nullable
        assertEquals("stuff['level1']['level2']", response.rows()[9][11]);
        assertEquals(false, response.rows()[9][21]); // is_nullable
        assertEquals("stuff['level1']['level2_nullable']", response.rows()[10][11]);
        assertEquals(true, response.rows()[10][21]); // is_nullable
    }

    @Test
    public void testSelectFromTableColumnsRefresh() {
        execute("create table test (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where table_schema = ?",
            new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(3L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);

        execute("create table test2 (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns " +
                "where table_schema = ? " +
                "order by table_name", new Object[]{sqlExecutor.getDefaultSchema()});

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
            new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(2L, response.rowCount());

        assertEquals("test", response.rows()[0][0]);
        assertEquals("col1", response.rows()[0][1]);
        short expected = 1;
        assertEquals(expected, response.rows()[0][2]);
        assertEquals("string", response.rows()[0][3]);

        assertEquals("test", response.rows()[1][0]);
        assertEquals("col2", response.rows()[1][1]);
        expected = 2;
        assertEquals(expected, response.rows()[1][2]);
        assertEquals("string", response.rows()[1][3]);
    }

    @Test
    public void testGlobalAggregation() {
        execute("select max(ordinal_position) from information_schema.columns");
        assertEquals(1, response.rowCount());

        short max_ordinal = 32;
        assertEquals(max_ordinal, response.rows()[0][0]);

        execute("create table t1 (id integer, col1 string)");
        ensureGreen();
        execute("select max(ordinal_position) from information_schema.columns where table_schema = ?",
            new Object[]{sqlExecutor.getDefaultSchema()});
        assertEquals(1, response.rowCount());

        max_ordinal = 2;
        assertEquals(max_ordinal, response.rows()[0][0]);
    }

    @Test
    public void testGlobalAggregationMany() {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=0)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=0)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=0)");
        ensureYellow();
        execute("select min(number_of_shards), max(number_of_shards), avg(number_of_shards)," +
                "sum(number_of_shards) from information_schema.tables where table_schema = ?", new Object[]{sqlExecutor.getDefaultSchema()});
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
            new Object[]{sqlExecutor.getDefaultSchema()});
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
        ensureYellow();
        execute("select count(*) from information_schema.tables");
        assertEquals(1, response.rowCount());
        assertEquals(28L, response.rows()[0][0]);
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
                "where table_schema = ? order by table_name", new Object[]{sqlExecutor.getDefaultSchema()});

        Object[] row1 = new String[]{"name", "content"};
        Object[] row2 = new String[]{"name"};
        assertThat((Object[]) response.rows()[0][6], arrayContaining(row1));
        assertThat((Object[]) response.rows()[1][6], arrayContaining(row2));
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

        execute("select table_name, schema_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions order by table_name, partition_ident");
        assertEquals(3, response.rowCount());

        Object[] row1 = new Object[]{"my_table", sqlExecutor.getDefaultSchema(), "04132", ImmutableMap.of("par", 1), 5, "0-1"};
        Object[] row2 = new Object[]{"my_table", sqlExecutor.getDefaultSchema(), "04134", ImmutableMap.of("par", 2), 5, "0-1"};
        Object[] row3 = new Object[]{"my_table", sqlExecutor.getDefaultSchema(), "04136", ImmutableMap.of("par", 3), 5, "0-1"};

        assertArrayEquals(row1, response.rows()[0]);
        assertArrayEquals(row2, response.rows()[1]);
        assertArrayEquals(row3, response.rows()[2]);
    }

    @Test
    public void testDisableWriteOnSinglePartition() {
        execute("create table my_table (par int, content string) " +
                "clustered into 5 shards " +
                "partitioned by (par)");
        execute("insert into my_table (par, content) values (1, 'content1'), " +
                "(1, 'content2'), " +
                "(2, 'content3'), " +
                "(2, 'content4'), " +
                "(2, 'content5'), " +
                "(3, 'content6')");

        ensureGreen();
        execute("alter table my_table partition (par=1) set (\"blocks.write\"=true)");

        // update is expected to be executed without exception since this partition has no write block
        execute("update my_table set content=\'content42\' where par=2");
        refresh();
        // verifying update
        execute("select content from my_table where par=2");
        assertThat(response.rowCount(), is(3L));

        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage("blocked by: [FORBIDDEN/8/index write (api)]");

        // trying to perform an update on a partition with a write block
        execute("update my_table set content=\'content42\' where par=1");

    }

    @Test
    public void testMultipleWritesWhenOnePartitionIsReadOnly() {
        execute("create table my_table (par int, content string) " +
                "clustered into 5 shards " +
                "partitioned by (par)");
        execute("insert into my_table (par, content) values " +
                "(1, 'content2'), " +
                "(2, 'content3')");

        ensureGreen();
        execute("alter table my_table partition (par=1) set (\"blocks.write\"=true)");
        try {
            execute("insert into my_table (par, content) values (2, 'content42'), " +
                    "(2, 'content42'), " +
                    "(1, 'content2'), " +
                    "(3, 'content6')");
            fail("expected to throw an \"blocked\" exception");
        } catch (SQLActionException e) {
            assertThat(e.getMessage(), containsString("blocked by: [FORBIDDEN/8/index write (api)];"));
        }
        refresh();
        execute("select * from my_table");
        assertThat(response.rowCount(), is(both(greaterThanOrEqualTo(2L)).and(lessThanOrEqualTo(5L))));
        //cleaning up
        execute("alter table my_table partition (par=1) set (\"blocks.write\"=false)");
    }

    @Test
    public void testPartitionedTableShardsAndReplicas() throws Exception {
        execute("create table parted (par byte, content string) " +
                "partitioned by (par) " +
                "clustered into 2 shards with (number_of_replicas=0)");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("parted| 2| 0\n"));

        execute("select * from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount(), is(0L));

        execute("insert into parted (par, content) values (1, 'foo'), (3, 'baz')");
        execute("refresh table parted");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("parted| 2| 0\n"));

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "parted| 04132| {par=1}| 2| 0\n" +
            "parted| 04136| {par=3}| 2| 0\n"));

        execute("alter table parted set (number_of_shards=6)");
        waitNoPendingTasksOnAll();

        execute("insert into parted (par, content) values (2, 'bar')");
        execute("refresh table parted");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("parted| 6| 0\n"));

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount(), is(3L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "parted| 04132| {par=1}| 2| 0\n" +
            "parted| 04134| {par=2}| 6| 0\n" +
            "parted| 04136| {par=3}| 2| 0\n"));

        execute("update parted set new=true where par=1");
        refresh();
        waitNoPendingTasksOnAll();

        // ensure newer index metadata does not override settings in template
        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("parted| 6| 0\n"));

        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount(), is(3L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "parted| 04132| {par=1}| 2| 0\n" +
            "parted| 04134| {par=2}| 6| 0\n" +
            "parted| 04136| {par=3}| 2| 0\n"));

        execute("delete from parted where par=2");
        waitNoPendingTasksOnAll();

        execute("select table_name, number_of_shards, number_of_replicas from information_schema.tables where table_name='parted'");
        assertThat(TestingHelpers.printedTable(response.rows()), is("parted| 6| 0\n"));
        waitNoPendingTasksOnAll();
        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions where table_name='parted' order by table_name, partition_ident");
        assertThat(response.rowCount(), is(2L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
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

        execute("select table_name, schema_name, partition_ident, values, number_of_shards, number_of_replicas " +
                "from information_schema.table_partitions order by table_name, partition_ident");
        assertEquals(5, response.rowCount());

        Object[] row1 = new Object[]{"my_table", sqlExecutor.getDefaultSchema(), "08132132c5p0", ImmutableMap.of("par", 1, "par_str", "bar"), 5, "0-1"};
        Object[] row2 = new Object[]{"my_table", sqlExecutor.getDefaultSchema(), "08132136dtng", ImmutableMap.of("par", 1, "par_str", "foo"), 5, "0-1"};
        Object[] row3 = new Object[]{"my_table", sqlExecutor.getDefaultSchema(), "08134132c5p0", ImmutableMap.of("par", 2, "par_str", "bar"), 5, "0-1"};
        Object[] row4 = new Object[]{"my_table", sqlExecutor.getDefaultSchema(), "08134136dtng", ImmutableMap.of("par", 2, "par_str", "foo"), 5, "0-1"};
        Object[] row5 = new Object[]{"my_table", sqlExecutor.getDefaultSchema(), "081341b1edi6c", ImmutableMap.of("par", 2, "par_str", "asdf"), 4, "0-1"};

        assertArrayEquals(row1, response.rows()[0]);
        assertArrayEquals(row2, response.rows()[1]);
        assertArrayEquals(row3, response.rows()[2]);
        assertArrayEquals(row4, response.rows()[3]);
        assertArrayEquals(row5, response.rows()[4]);
    }

    @Test
    public void testPartitionsNestedCol() {
        execute("create table my_table (id int, metadata object as (date timestamp)) " +
                "clustered into 5 shards " +
                "partitioned by (metadata['date'])");
        ensureYellow();
        execute("insert into my_table (id, metadata) values (?, ?), (?, ?)",
            new Object[]{
                1, new MapBuilder<String, Object>().put("date", "1970-01-01").map(),
                2, new MapBuilder<String, Object>().put("date", "2014-05-28").map()
            });
        refresh();

        execute("select table_name, partition_ident, values from information_schema.table_partitions order by table_name, partition_ident");
        assertEquals(2, response.rowCount());

        assertThat(TestingHelpers.printedTable(response.rows()),
            is("my_table| 04130| {metadata['date']=0}\n" +
               "my_table| 04732d1g64p36d9i60o30c1g| {metadata['date']=1401235200000}\n"));
    }

    @Test
    public void testAnyInformationSchema() {
        execute("create table any1 (id integer, date timestamp, names array(string)) partitioned by (date)");
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
                          "data_points| data['somelongroute']| long\n" +
                          "data_points| data['somestringroute']| string\n" +
                          "data_points| day| string\n";
        assertEquals(expected, TestingHelpers.printedTable(response.rows()));
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
    public void testOrphanedPartition() throws Exception {
        prepareCreate(".partitioned.foo.04138").execute().get();
        execute("select table_name from information_schema.tables where table_schema='doc'");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testSelectGeneratedColumnFromInformationSchemaColumns() {
        execute("create table t (lastname string, firstname string, name as (lastname || '_' || firstname)) " +
                "with (number_of_replicas = 0)");
        execute("select column_name, is_generated, generation_expression from information_schema.columns where is_generated = true");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("name| true| concat(concat(lastname, '_'), firstname)\n"));
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
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(" / by zero");
        execute("select 1/0 from information_schema.tables");
    }

    @Test
    public void testInformationRoutinesColumns() {
        execute("select column_name from information_schema.columns where table_name='routines' order by ordinal_position");
        assertThat(TestingHelpers.printedTable(response.rows()),
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
        ensureYellow();
        String partitionIdent = client().admin().indices().getIndex(new GetIndexRequest()).actionGet().getIndices()[0];

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

        final String defaultSchema = sqlExecutor.getDefaultSchema();
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

}
