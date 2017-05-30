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
import io.crate.testing.UseJdbc;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;


@ESIntegTestCase.ClusterScope(numDataNodes = 2)
@UseJdbc
public class InformationSchemaTest extends SQLTransportIntegrationTest {

    private final Joiner commaJoiner = Joiner.on(", ");

    private void serviceSetup() {
        execute("create table t1 (col1 integer primary key, " +
                "col2 string) clustered into 7 " +
                "shards");
        execute("create table t2 (col1 integer primary key, " +
                "col2 string) clustered into " +
                "10 shards");
        execute(
            "create table t3 (col1 integer, col2 string) clustered into 5 shards with (number_of_replicas=8)");
        ensureYellow();
    }

    @Test
    public void testDefaultTables() throws Exception {
        execute("select * from information_schema.tables order by table_schema, table_name");
        assertEquals(20L, response.rowCount());

        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| information_schema| columns| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| information_schema| routines| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| information_schema| schemata| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| information_schema| sql_features| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| information_schema| table_constraints| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| information_schema| table_partitions| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| information_schema| tables| information_schema| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| pg_catalog| pg_type| pg_catalog| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| checks| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| cluster| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| jobs| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| jobs_log| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| node_checks| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| nodes| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| operations| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| operations_log| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| repositories| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| shards| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| snapshots| sys| BASE TABLE| NULL\n" +
            "NULL| NULL| NULL| strict| 0| 1| NULL| NULL| NULL| NULL| NULL| sys| summits| sys| BASE TABLE| NULL\n"));
    }

    @Test
    public void testSelectFromInformationSchema() throws Exception {
        execute("create table quotes (" +
                "id integer primary key, " +
                "quote string index off, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='snowball')" +
                ") clustered by (id) into 3 shards with (number_of_replicas=10)");

        execute("select table_name, number_of_shards, number_of_replicas, clustered_by from " +
                "information_schema.tables " +
                "where table_name='quotes'");
        assertEquals(1L, response.rowCount());
        assertEquals("quotes", response.rows()[0][0]);
        assertEquals(3, response.rows()[0][1]);
        assertEquals("10", response.rows()[0][2]);
        assertEquals("id", response.rows()[0][3]);

        execute("select * from information_schema.columns where table_name='quotes'");
        assertEquals(2L, response.rowCount());


        execute("select * from information_schema.table_constraints where table_schema='doc' and table_name='quotes'");
        assertEquals(1L, response.rowCount());

        execute("select * from information_schema.routines");
        assertEquals(119L, response.rowCount());
    }

    @Test
    public void testSearchInformationSchemaTablesRefresh() throws Exception {
        serviceSetup();

        execute("select * from information_schema.tables");
        assertEquals(23L, response.rowCount());

        execute("create table t4 (col1 integer, col2 string) with (number_of_replicas=0)");
        ensureGreen("t4");

        execute("select * from information_schema.tables");
        assertEquals(24L, response.rowCount());
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderBy() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        execute("create table foo (col1 integer primary key, " +
                "col2 string) clustered by(col1) into 3 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema='doc' order by table_name asc");
        assertThat(response.rowCount(), is(2L));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[0][13], is("doc"));
        assertThat(response.rows()[0][12], is("foo"));
        assertThat(response.rows()[0][11], is("doc"));
        assertThat(response.rows()[0][14], is("BASE TABLE"));
        assertThat(response.rows()[0][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        assertThat(response.rows()[0][5], is(3));
        assertThat(response.rows()[0][4], is("1"));
        assertThat(response.rows()[0][2], is("col1"));
        assertThat(response.rows()[0][1], is(false));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[1][13], is("doc"));
        assertThat(response.rows()[1][12], is("test"));
        assertThat(response.rows()[1][11], is("doc"));
        assertThat(response.rows()[1][14], is("BASE TABLE"));
        assertThat(response.rows()[1][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        assertThat(response.rows()[1][5], is(5));
        assertThat(response.rows()[1][4], is("1"));
        assertThat(response.rows()[1][2], is("col1"));
        assertThat(response.rows()[0][1], is(false));
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimit() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select table_schema, table_name, number_of_shards, number_of_replicas " +
                "from INFORMATION_SCHEMA.Tables where table_schema='doc' " +
                "order by table_name asc limit 1");
        assertEquals(1L, response.rowCount());
        assertEquals("doc", response.rows()[0][0]);
        assertEquals("foo", response.rows()[0][1]);
        assertEquals(3, response.rows()[0][2]);
        assertEquals("1", response.rows()[0][3]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByTwoColumnsAndLimit() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) clustered into 1 shards");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        execute("create table bar (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select table_name, number_of_shards from INFORMATION_SCHEMA.Tables where table_schema='doc' " +
                "order by number_of_shards desc, table_name asc limit 2");
        assertEquals(2L, response.rowCount());

        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "bar| 3\n" +
            "foo| 3\n"));
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimitOffset() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema='doc' order by table_name asc limit 1 offset 1");
        assertThat(response.rowCount(), is(1L));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null); // version
        assertThat(response.rows()[0][13], is("doc")); // table_schema
        assertThat(response.rows()[0][12], is("test"));  // table_name
        assertThat(response.rows()[0][11], is("doc")); // table_catalog
        assertThat(response.rows()[0][14], is("BASE TABLE")); // table_type
        assertThat(response.rows()[0][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME)); // routing_hash_function
        assertThat(response.rows()[0][5], is(5)); // number_of_shards
        assertThat(response.rows()[0][4], is("1")); // number_of_replicas
        assertThat(response.rows()[0][2], is("col1")); // primary key
        assertThat(response.rows()[0][1], is(false)); // closed
    }

    @Test
    public void testSelectFromInformationSchemaTable() throws Exception {
        execute("select TABLE_NAME from INFORMATION_SCHEMA.Tables where table_schema='doc'");
        assertEquals(0L, response.rowCount());

        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by from INFORMATION_SCHEMA.Tables where table_schema='doc'");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals("1", response.rows()[0][2]);
        assertEquals("col1", response.rows()[0][3]);
    }

    @Test
    public void testSelectBlobTablesFromInformationSchemaTable() throws Exception {
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
        assertEquals("1", response.rows()[0][2]);
        assertEquals("digest", response.rows()[0][3]);
        assertEquals(blobsPath, response.rows()[0][4]);
        assertThat(response.rows()[0][5], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        TestingHelpers.assertCrateVersion(response.rows()[0][6], Version.CURRENT, null);

        // cleanup blobs path, tempDir hook will be deleted before table would be deleted, avoid error in log
        execute("drop blob table test");
    }

    @Test
    public void testSelectPartitionedTablesFromInformationSchemaTable() throws Exception {
        execute("create table test (id int primary key, name string) partitioned by (id)");
        execute("insert into test (id, name) values (1, 'Youri'), (2, 'Ruben')");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by, partitioned_by from INFORMATION_SCHEMA.Tables where table_schema = 'doc'");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(4, response.rows()[0][1]);
        assertEquals("1", response.rows()[0][2]);
        assertEquals("id", response.rows()[0][3]);
        assertThat((Object[]) response.rows()[0][4], arrayContaining(new Object[]{"id"}));
    }

    @Test
    public void testSelectStarFromInformationSchemaTable() throws Exception {
        execute("create table test (col1 integer, col2 string) clustered into 5 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where table_schema='doc'");
        assertThat(response.rowCount(), is(1L));

        TestingHelpers.assertCrateVersion(response.rows()[0][15], Version.CURRENT, null);
        assertThat(response.rows()[0][13], is("doc"));
        assertThat(response.rows()[0][12], is("test"));
        assertThat(response.rows()[0][11], is("doc"));
        assertThat(response.rows()[0][14], is("BASE TABLE"));
        assertThat(response.rows()[0][8], is(IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION_PRETTY_NAME));
        assertThat(response.rows()[0][5], is(5));
        assertThat(response.rows()[0][4], is("1"));
        assertThat(response.rows()[0][2], is("_id"));
        assertThat(response.rows()[0][1], is(false));
    }

    @Test
    public void testSelectFromTableConstraints() throws Exception {

        execute("select * from INFORMATION_SCHEMA.table_constraints order by table_schema asc, table_name asc");
        assertEquals(13L, response.rowCount());
        assertThat(response.cols(),
            arrayContaining("constraint_name", "constraint_type", "table_name", "table_schema"));
        assertThat(TestingHelpers.printedTable(response.rows()),
            is(
            "[table_name, table_schema, column_name]| PRIMARY_KEY| columns| information_schema\n" +
            "[schema_name]| PRIMARY_KEY| schemata| information_schema\n" +
            "[feature_id, feature_name, sub_feature_id, sub_feature_name, is_supported, is_verified_by, comments]| PRIMARY_KEY| sql_features| information_schema\n" +
            "[table_schema, table_name]| PRIMARY_KEY| tables| information_schema\n" +
            "[id]| PRIMARY_KEY| checks| sys\n" +
            "[id]| PRIMARY_KEY| jobs| sys\n" +
            "[id]| PRIMARY_KEY| jobs_log| sys\n" +
            "[id, node_id]| PRIMARY_KEY| node_checks| sys\n" +
            "[id]| PRIMARY_KEY| nodes| sys\n" +
            "[name]| PRIMARY_KEY| repositories| sys\n" +
            "[schema_name, table_name, id, partition_ident]| PRIMARY_KEY| shards| sys\n" +
            "[name, repository]| PRIMARY_KEY| snapshots| sys\n" +
            "[mountain]| PRIMARY_KEY| summits| sys\n"
        ));

        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();
        execute("select constraint_type, constraint_name, " +
                "table_name from information_schema.table_constraints where table_schema='doc'");
        assertEquals(1L, response.rowCount());
        assertEquals("PRIMARY_KEY", response.rows()[0][0]);
        assertThat(commaJoiner.join((Object[]) response.rows()[0][1]), is("col1"));
        assertEquals("test", response.rows()[0][2]);
    }

    @Test
    public void testRefreshTableConstraints() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();
        execute("select table_name, constraint_name from INFORMATION_SCHEMA" +
                ".table_constraints where table_schema='doc'");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertThat(commaJoiner.join((Object[]) response.rows()[0][1]), is("col1"));

        execute("create table test2 (col1a string primary key, col2a timestamp)");
        ensureGreen();
        execute("select table_name, constraint_name from INFORMATION_SCHEMA.table_constraints where table_schema='doc' order by table_name asc");

        assertEquals(2L, response.rowCount());
        assertEquals("test2", response.rows()[1][0]);
        assertThat(commaJoiner.join((Object[]) response.rows()[1][1]), is("col1a"));
    }

    @Test
    public void testSelectFromRoutines() throws Exception {
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
    public void testSelectAnalyzersFromRoutines() throws Exception {
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
    public void testSelectTokenizersFromRoutines() throws Exception {
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
    public void testSelectTokenFiltersFromRoutines() throws Exception {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
                "routine_type='TOKEN_FILTER' order by " +
                "routine_name asc limit 5");
        assertEquals(5L, response.rowCount());
        String[] tokenFilterNames = new String[response.rows().length];
        for (int i = 0; i < response.rowCount(); i++) {
            tokenFilterNames[i] = (String) response.rows()[i][0];
        }
        assertEquals(
            "apostrophe, arabic_normalization, arabic_stem, asciifolding, brazilian_stem",
            Joiner.on(", ").join(tokenFilterNames)
        );
    }

    @Test
    public void testSelectCharFiltersFromRoutines() throws Exception {
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
    public void testTableConstraintsWithOrderBy() throws Exception {
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
    public void testDefaultColumns() throws Exception {
        execute("select * from information_schema.columns order by table_schema, table_name");
        assertEquals(414, response.rowCount());
    }

    @Test
    public void testColumnsColumns() throws Exception {
        execute("select * from information_schema.columns where table_schema='information_schema' and table_name='columns' order by ordinal_position asc");
        assertThat(response.rowCount(), is(32L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_maximum_length| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 31| 2| NULL| 1| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_octet_length| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 31| 2| NULL| 2| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_set_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 3| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_set_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 4| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| character_set_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 5| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| check_action| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 31| 2| NULL| 6| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| check_references| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 7| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| collation_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 8| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| collation_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 9| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| collation_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 10| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| column_default| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 11| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| column_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 12| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| data_type| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 13| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| datetime_precision| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 31| 2| NULL| 14| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| domain_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 15| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| domain_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 16| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| domain_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 17| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| generation_expression| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 18| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| interval_precision| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 31| 2| NULL| 19| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| interval_type| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 20| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| is_generated| boolean| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 21| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| is_nullable| boolean| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 22| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| numeric_precision| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 31| 2| NULL| 23| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| numeric_precision_radix| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 31| 2| NULL| 24| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| numeric_scale| integer| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| 31| 2| NULL| 25| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| ordinal_position| short| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| 15| 2| NULL| 26| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| table_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 27| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| table_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 28| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| table_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| false| NULL| NULL| NULL| 29| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| user_defined_type_catalog| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 30| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| user_defined_type_name| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 31| columns| columns| information_schema| NULL| NULL| NULL\n" +
            "NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| NULL| user_defined_type_schema| string| NULL| NULL| NULL| NULL| NULL| NULL| NULL| false| true| NULL| NULL| NULL| 32| columns| columns| information_schema| NULL| NULL| NULL\n")
        );
    }

    @Test
    public void testSelectFromTableColumns() throws Exception {
        execute("create table test (col1 integer primary key, col2 string index off, age integer not null)");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Columns where table_schema='doc'");
        assertEquals(3L, response.rowCount());
        assertEquals("age", response.rows()[0][11]);
        assertEquals("integer", response.rows()[0][12]);
        assertEquals(null, response.rows()[0][13]);
        assertEquals(false, response.rows()[0][20]);
        assertEquals(false, response.rows()[0][21]);

        assertEquals((short) 1, response.rows()[0][25]);
        assertEquals("test", response.rows()[0][26]);
        assertEquals("doc", response.rows()[0][28]);

        assertEquals("col1", response.rows()[1][11]);
        assertEquals(false, response.rows()[1][21]);

        assertEquals("col2", response.rows()[2][11]);
        assertEquals(true, response.rows()[2][21]);
    }

    @Test
    public void testSelectFromTableColumnsRefresh() throws Exception {
        execute("create table test (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where table_schema='doc'");
        assertEquals(3L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);

        execute("create table test2 (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns " +
                "where table_schema='doc' " +
                "order by table_name");

        assertEquals(6L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals("test2", response.rows()[4][0]);
    }

    @Test
    public void testSelectFromTableColumnsMultiField() throws Exception {
        execute("create table test (col1 string, col2 string," +
                "index col1_col2_ft using fulltext(col1, col2))");
        ensureGreen();
        execute("select table_name, column_name," +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where table_schema='doc'");
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
    public void testGlobalAggregation() throws Exception {
        execute("select max(ordinal_position) from information_schema.columns");
        assertEquals(1, response.rowCount());

        short max_ordinal = 32;
        assertEquals(max_ordinal, response.rows()[0][0]);

        execute("create table t1 (id integer, col1 string)");
        ensureGreen();
        execute("select max(ordinal_position) from information_schema.columns where table_schema='doc'");
        assertEquals(1, response.rowCount());

        max_ordinal = 2;
        assertEquals(max_ordinal, response.rows()[0][0]);
    }

    @Test
    public void testGlobalAggregationMany() throws Exception {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=14)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=7)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=2)");
        ensureYellow();
        execute("select min(number_of_shards), max(number_of_shards), avg(number_of_shards)," +
                "sum(number_of_shards) from information_schema.tables where table_schema='doc'");
        assertEquals(1, response.rowCount());

        assertEquals(3, response.rows()[0][0]);
        assertEquals(10, response.rows()[0][1]);
        assertEquals(6.0d, response.rows()[0][2]);
        assertEquals(18.0d, response.rows()[0][3]);
    }

    @Test
    public void testGlobalAggregationWithWhere() throws Exception {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=14)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=7)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=2)");
        ensureYellow();
        execute("select min(number_of_shards), max(number_of_shards), avg(number_of_shards)," +
                "sum(number_of_shards) from information_schema.tables where table_schema='doc' and table_name != 't1'");
        assertEquals(1, response.rowCount());

        assertEquals(3, response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals(4.0d, response.rows()[0][2]);
        assertEquals(8.0d, response.rows()[0][3]);
    }

    @Test
    public void testGlobalAggregationWithAlias() throws Exception {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=14)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=7)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=2)");
        ensureYellow();
        execute("select min(number_of_shards) as min_shards from information_schema.tables where table_name = 't1'");
        assertEquals(1, response.rowCount());

        assertEquals(10, response.rows()[0][0]);
    }

    @Test
    public void testGlobalCount() throws Exception {
        execute("create table t1 (id integer, col1 string) clustered into 10 shards with(number_of_replicas=14)");
        execute("create table t2 (id integer, col1 string) clustered into 5 shards with(number_of_replicas=7)");
        execute("create table t3 (id integer, col1 string) clustered into 3 shards with(number_of_replicas=2)");
        ensureYellow();
        execute("select count(*) from information_schema.tables");
        assertEquals(1, response.rowCount());
        assertEquals(23L, response.rows()[0][0]);
    }

    @Test
    public void testGlobalCountDistinct() throws Exception {
        execute("create table t3 (id integer, col1 string)");
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
    public void testAddColumnToIgnoredObject() throws Exception {
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
    public void testPartitionedBy() throws Exception {
        execute("create table my_table (id integer, name string) partitioned by (name)");
        execute("create table my_other_table (id integer, name string, content string) " +
                "partitioned by (name, content)");

        execute("select * from information_schema.tables " +
                "where table_schema = 'doc' order by table_name");

        Object[] row1 = new String[]{"name", "content"};
        Object[] row2 = new String[]{"name"};
        assertThat((Object[]) response.rows()[0][6], arrayContaining(row1));
        assertThat((Object[]) response.rows()[1][6], arrayContaining(row2));
    }

    @Test
    public void testTablePartitions() throws Exception {
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

        Object[] row1 = new Object[]{"my_table", "doc", "04132", ImmutableMap.of("par", 1), 5, "1"};
        Object[] row2 = new Object[]{"my_table", "doc", "04134", ImmutableMap.of("par", 2), 5, "1"};
        Object[] row3 = new Object[]{"my_table", "doc", "04136", ImmutableMap.of("par", 3), 5, "1"};

        assertArrayEquals(row1, response.rows()[0]);
        assertArrayEquals(row2, response.rows()[1]);
        assertArrayEquals(row3, response.rows()[2]);
    }

    @Test
    public void testDisableWriteOnSinglePartition() throws Exception {
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
    public void testMultipleWritesWhenOnePartitionIsReadOnly() throws Exception {
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

        Object[] row1 = new Object[]{"my_table", "doc", "08132132c5p0", ImmutableMap.of("par", 1, "par_str", "bar"), 5, "1"};
        Object[] row2 = new Object[]{"my_table", "doc", "08132136dtng", ImmutableMap.of("par", 1, "par_str", "foo"), 5, "1"};
        Object[] row3 = new Object[]{"my_table", "doc", "08134132c5p0", ImmutableMap.of("par", 2, "par_str", "bar"), 5, "1"};
        Object[] row4 = new Object[]{"my_table", "doc", "08134136dtng", ImmutableMap.of("par", 2, "par_str", "foo"), 5, "1"};
        Object[] row5 = new Object[]{"my_table", "doc", "081341b1edi6c", ImmutableMap.of("par", 2, "par_str", "asdf"), 4, "1"};

        assertArrayEquals(row1, response.rows()[0]);
        assertArrayEquals(row2, response.rows()[1]);
        assertArrayEquals(row3, response.rows()[2]);
        assertArrayEquals(row4, response.rows()[3]);
        assertArrayEquals(row5, response.rows()[4]);
    }

    @Test
    public void testPartitionsNestedCol() throws Exception {
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
    public void testAnyInformationSchema() throws Exception {
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
    public void testRegexpMatch() throws Exception {
        serviceSetup();
        execute("create blob table blob_t1 with (number_of_replicas=0)");
        ensureYellow();
        execute("select distinct table_schema from information_schema.tables " +
                "where table_schema ~ '[a-z]+o[a-z]' order by table_schema");
        assertThat(response.rowCount(), is(2L));
        assertThat(response.rows()[0][0], is("blob"));
        assertThat(response.rows()[1][0], is("doc"));
    }

    @Test
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
    public void testSelectGeneratedColumnFromInformationSchemaColumns() throws Exception {
        execute("create table t (lastname string, firstname string, name as (lastname || '_' || firstname)) " +
                "with (number_of_replicas = 0)");
        execute("select column_name, is_generated, generation_expression from information_schema.columns where is_generated = true");
        assertThat(TestingHelpers.printedTable(response.rows()),
            is("name| true| concat(concat(lastname, '_'), firstname)\n"));
    }

    @Test
    public void testSelectSqlFeatures() throws Exception {
        execute("select * from information_schema.sql_features order by feature_id asc");
        assertThat(response.rowCount(), is(672L));

        execute("select feature_id, feature_name from information_schema.sql_features where feature_id='E011'");
        assertThat(response.rowCount(), is(7L));
        assertThat(response.rows()[0][1], is("Numeric data types"));
    }

    @Test
    public void testScalarEvaluatesInErrorOnInformationSchema() throws Exception {
        expectedException.expect(SQLActionException.class);
        expectedException.expectMessage(" / by zero");
        execute("select 1/0 from information_schema.tables");
    }

    @Test
    public void testInformationRoutinesColumns() throws Exception {
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
    public void testOpenCloseTableInformation() throws Exception {
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
    public void testOpenClosePartitionInformation() throws Exception {
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
}
