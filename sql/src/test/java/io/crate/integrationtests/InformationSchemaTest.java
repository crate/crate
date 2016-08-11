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
import com.google.common.collect.ImmutableSet;
import io.crate.action.sql.SQLAction;
import io.crate.action.sql.SQLActionException;
import io.crate.action.sql.SQLRequest;
import io.crate.testing.TestingHelpers;
import io.crate.testing.UseJdbc;
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

    final static Joiner commaJoiner = Joiner.on(", ");

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
        execute("select * from information_schema.tables order by schema_name, table_name");
        assertEquals(19L, response.rowCount());

        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "NULL| NULL| strict| 0| 1| NULL| information_schema| NULL| columns\n" +
                "NULL| NULL| strict| 0| 1| NULL| information_schema| NULL| routines\n" +
                "NULL| NULL| strict| 0| 1| NULL| information_schema| NULL| schemata\n" +
                "NULL| NULL| strict| 0| 1| NULL| information_schema| NULL| sql_features\n" +
                "NULL| NULL| strict| 0| 1| NULL| information_schema| NULL| table_constraints\n" +
                "NULL| NULL| strict| 0| 1| NULL| information_schema| NULL| table_partitions\n" +
                "NULL| NULL| strict| 0| 1| NULL| information_schema| NULL| tables\n" +
                "NULL| NULL| strict| 0| 1| NULL| pg_catalog| NULL| pg_type\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| checks\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| cluster\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| jobs\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| jobs_log\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| node_checks\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| nodes\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| operations\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| operations_log\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| repositories\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| shards\n" +
                "NULL| NULL| strict| 0| 1| NULL| sys| NULL| snapshots\n"));
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


        execute("select * from information_schema.table_constraints where schema_name='doc' and table_name='quotes'");
        assertEquals(1L, response.rowCount());

        execute("select * from information_schema.routines");
        assertEquals(118L, response.rowCount());
    }

    @Test
    public void testSearchInformationSchemaTablesRefresh() throws Exception {
        serviceSetup();

        execute("select * from information_schema.tables");
        assertEquals(22L, response.rowCount());

        client().execute(SQLAction.INSTANCE,
            new SQLRequest("create table t4 (col1 integer, col2 string) with (number_of_replicas=0)")).actionGet();
        ensureGreen("t4");

        execute("select * from information_schema.tables");
        assertEquals(23L, response.rowCount());
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderBy() throws Exception {
        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        execute("create table foo (col1 integer primary key, " +
                "col2 string) clustered by(col1) into 3 shards");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Tables where schema_name='doc' order by table_name asc");
        assertEquals(2L, response.rowCount());
        assertEquals("doc", response.rows()[0][6]);
        assertEquals("foo", response.rows()[0][8]);
        assertEquals(3, response.rows()[0][4]); //
        assertEquals("1", response.rows()[0][3]);
        assertEquals("col1", response.rows()[0][1]);

        assertEquals("doc", response.rows()[1][6]);
        assertEquals("test", response.rows()[1][8]);
        assertEquals(5, response.rows()[1][4]);
        assertEquals("1", response.rows()[1][3]);
        assertEquals("col1", response.rows()[1][1]);
    }

    @Test
    public void testSelectStarFromInformationSchemaTableWithOrderByAndLimit() throws Exception {
        execute("create table test (col1 integer primary key, col2 string)");
        execute("create table foo (col1 integer primary key, col2 string) clustered into 3 shards");
        ensureGreen();
        execute("select schema_name, table_name, number_of_shards, number_of_replicas " +
                "from INFORMATION_SCHEMA.Tables where schema_name='doc' " +
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
        execute("select table_name, number_of_shards from INFORMATION_SCHEMA.Tables where schema_name='doc' " +
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
        execute("select * from INFORMATION_SCHEMA.Tables where schema_name='doc' order by table_name asc limit 1 offset 1");
        assertEquals(1L, response.rowCount());

        assertEquals("doc", response.rows()[0][6]); // schema_name
        assertEquals("test", response.rows()[0][8]); // table_name
        assertEquals(5, response.rows()[0][4]); // number_of_shards
        assertEquals("1", response.rows()[0][3]); // number_of_replicas
        assertEquals("col1", response.rows()[0][1]); // primary key
    }

    @Test
    public void testSelectFromInformationSchemaTable() throws Exception {
        execute("select TABLE_NAME from INFORMATION_SCHEMA.Tables where schema_name='doc'");
        assertEquals(0L, response.rowCount());

        execute("create table test (col1 integer primary key, col2 string) clustered into 5 shards");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by from INFORMATION_SCHEMA.Tables where schema_name='doc'");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals("1", response.rows()[0][2]);
        assertEquals("col1", response.rows()[0][3]);
    }

    @Test
    public void testSelectBlobTablesFromInformationSchemaTable() throws Exception {
        execute("select TABLE_NAME from INFORMATION_SCHEMA.Tables where schema_name='blob'");
        assertEquals(0L, response.rowCount());

        String blobsPath = createTempDir().toAbsolutePath().toString();
        execute("create blob table test clustered into 5 shards with (blobs_path=?)", new Object[]{blobsPath});
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by, blobs_path from INFORMATION_SCHEMA.Tables where schema_name='blob' ");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertEquals(5, response.rows()[0][1]);
        assertEquals("1", response.rows()[0][2]);
        assertEquals("digest", response.rows()[0][3]);
        assertEquals(blobsPath, response.rows()[0][4]);

        // cleanup blobs path, tempDir hook will be deleted before table would be deleted, avoid error in log
        execute("drop blob table test");
    }

    @Test
    public void testSelectPartitionedTablesFromInformationSchemaTable() throws Exception {
        execute("create table test (id int primary key, name string) partitioned by (id)");
        execute("insert into test (id, name) values (1, 'Youri'), (2, 'Ruben')");
        ensureGreen();

        execute("select table_name, number_of_shards, number_of_replicas, " +
                "clustered_by, partitioned_by from INFORMATION_SCHEMA.Tables where schema_name = 'doc'");
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
        execute("select * from INFORMATION_SCHEMA.Tables where schema_name='doc'");
        assertEquals(1L, response.rowCount());
        assertEquals("doc", response.rows()[0][6]);
        assertEquals("test", response.rows()[0][8]);
        assertEquals(5, response.rows()[0][4]);
        assertEquals("1", response.rows()[0][3]);
        assertEquals("_id", response.rows()[0][1]);
    }

    @Test
    public void testSelectFromTableConstraints() throws Exception {

        execute("select * from INFORMATION_SCHEMA.table_constraints order by schema_name asc, table_name asc");
        assertEquals(12L, response.rowCount());
        assertThat(response.cols(),
                arrayContaining("constraint_name", "constraint_type", "schema_name", "table_name"));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "[schema_name, table_name, column_name]| PRIMARY_KEY| information_schema| columns\n" +
                "[schema_name]| PRIMARY_KEY| information_schema| schemata\n" +
                "[feature_id, feature_name, sub_feature_id, sub_feature_name, is_supported, is_verified_by, comments]| PRIMARY_KEY| information_schema| sql_features\n" +
                "[schema_name, table_name]| PRIMARY_KEY| information_schema| tables\n" +
                "[id]| PRIMARY_KEY| sys| checks\n" +
                "[id]| PRIMARY_KEY| sys| jobs\n" +
                "[id]| PRIMARY_KEY| sys| jobs_log\n" +
                "[id]| PRIMARY_KEY| sys| node_checks\n" +
                "[id]| PRIMARY_KEY| sys| nodes\n" +
                "[name]| PRIMARY_KEY| sys| repositories\n" +
                "[schema_name, table_name, id, partition_ident]| PRIMARY_KEY| sys| shards\n" +
                "[name, repository]| PRIMARY_KEY| sys| snapshots\n"
        ));

        execute("create table test (col1 integer primary key, col2 string)");
        ensureGreen();
        execute("select constraint_type, constraint_name, " +
                "table_name from information_schema.table_constraints where schema_name='doc'");
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
                ".table_constraints where schema_name='doc'");
        assertEquals(1L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);
        assertThat(commaJoiner.join((Object[]) response.rows()[0][1]), is("col1"));

        execute("create table test2 (col1a string primary key, col2a timestamp)");
        ensureGreen();
        execute("select table_name, constraint_name from INFORMATION_SCHEMA.table_constraints where schema_name='doc' order by table_name asc");

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
        execute("SELECT * from INFORMATION_SCHEMA.routines " +
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
                .setPersistentSettingsToRemove(
                    ImmutableSet.of("crate.analysis.custom.analyzer.myanalyzer",
                            "crate.analysis.custom.analyzer.myotheranalyzer",
                            "crate.analysis.custom.filter.myanalyzer_mytokenfilter"))
                .setTransientSettingsToRemove(
                        ImmutableSet.of("crate.analysis.custom.analyzer.myanalyzer",
                                "crate.analysis.custom.analyzer.myotheranalyzer",
                                "crate.analysis.custom.filter.myanalyzer_mytokenfilter"))
                .execute().actionGet();
    }

    @Test
    public void testSelectAnalyzersFromRoutines() throws Exception {
        execute("SELECT routine_name from INFORMATION_SCHEMA.routines WHERE " +
               "routine_type='ANALYZER' order by " +
                "routine_name desc limit 5");
        assertEquals(5L, response.rowCount());
        String[] analyzerNames = new String[response.rows().length];
        for (int i=0; i<response.rowCount(); i++) {
            analyzerNames[i] = (String)response.rows()[i][0];
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
        for (int i=0; i<response.rowCount(); i++) {
            tokenizerNames[i] = (String)response.rows()[i][0];
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
        for (int i=0; i<response.rowCount(); i++) {
            tokenFilterNames[i] = (String)response.rows()[i][0];
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
        assertEquals(4L, response.rowCount());
        String[] charFilterNames = new String[response.rows().length];
        for (int i=0; i<response.rowCount(); i++) {
            charFilterNames[i] = (String)response.rows()[i][0];
        }
        assertEquals(
                "htmlStrip, html_strip, mapping, pattern_replace",
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
                "where schema_name not in ('sys', 'information_schema')  " +
                "ORDER BY table_name");
        assertEquals(3L, response.rowCount());
        assertEquals("abc", response.rows()[0][0]);
        assertEquals("test1", response.rows()[1][0]);
        assertEquals("test2", response.rows()[2][0]);
    }

    @Test
    public void testDefaultColumns() throws Exception {
        execute("select * from information_schema.columns order by schema_name, table_name");
        assertEquals(349L, response.rowCount());
    }

    @Test
    public void testColumnsColumns() throws Exception {
        execute("select * from information_schema.columns where schema_name='information_schema' and table_name='columns' order by ordinal_position asc");
        assertThat(response.rowCount(), is(8L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "column_name| string| NULL| false| false| 1| information_schema| columns\n" +
                "data_type| string| NULL| false| true| 2| information_schema| columns\n" +
                "generation_expression| string| NULL| false| true| 3| information_schema| columns\n" +
                "is_generated| boolean| NULL| false| true| 4| information_schema| columns\n" +
                "is_nullable| boolean| NULL| false| true| 5| information_schema| columns\n" +
                "ordinal_position| short| NULL| false| true| 6| information_schema| columns\n" +
                "schema_name| string| NULL| false| false| 7| information_schema| columns\n" +
                "table_name| string| NULL| false| false| 8| information_schema| columns\n"));
    }

    @Test
    public void testSelectFromTableColumns() throws Exception {
        execute("create table test (col1 integer primary key, col2 string index off, age integer not null)");
        ensureGreen();
        execute("select * from INFORMATION_SCHEMA.Columns where schema_name='doc'");
        assertEquals(3L, response.rowCount());
        assertEquals("age", response.rows()[0][0]);
        assertEquals("integer", response.rows()[0][1]);
        assertEquals(null, response.rows()[0][2]);
        assertEquals(false, response.rows()[0][3]);
        assertEquals(false, response.rows()[0][4]);
        short expected = 1;
        assertEquals(expected, response.rows()[0][5]);
        assertEquals("doc", response.rows()[0][6]);
        assertEquals("test", response.rows()[0][7]);

        assertEquals("col1", response.rows()[1][0]);
        assertEquals(false, response.rows()[1][4]);

        assertEquals("col2", response.rows()[2][0]);
        assertEquals(true, response.rows()[2][4]);
    }

    @Test
    public void testSelectFromTableColumnsRefresh() throws Exception {
        execute("create table test (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where schema_name='doc'");
        assertEquals(3L, response.rowCount());
        assertEquals("test", response.rows()[0][0]);

        execute("create table test2 (col1 integer, col2 string, age integer)");
        ensureGreen();
        execute("select table_name, column_name, " +
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns " +
                "where schema_name='doc' " +
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
                "ordinal_position, data_type from INFORMATION_SCHEMA.Columns where schema_name='doc'");
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

    /* TODO: enable when information_schema.indices is implemented
    @SuppressWarnings("unchecked")
    @Test
    public void testSelectFromTableIndices() throws Exception {
        execute("create table test (col1 string, col2 string, " +
                "col3 string index using fulltext, " +
                "col4 string index off, " +
                "index col1_col2_ft using fulltext(col1, col2) with(analyzer='english'))");
        ensureGreen();
        execute("select table_name, index_name, method, columns, properties " +
                "from INFORMATION_SCHEMA.Indices");
        assertEquals(4L, response.rowCount());

        assertEquals("test", response.rows()[0][0]);
        assertEquals("col1", response.rows()[0][1]);
        assertEquals("plain", response.rows()[0][2]);
        assertTrue(response.rows()[0][3] instanceof List);
        assertThat((List<String>) response.rows()[0][3], contains("col1"));
        assertEquals("", response.rows()[0][4]);

        assertEquals("test", response.rows()[1][0]);
        assertEquals("col2", response.rows()[1][1]);
        assertEquals("plain", response.rows()[1][2]);
        assertThat((List<String>) response.rows()[1][3], contains("col2"));
        assertEquals("", response.rows()[1][4]);

        assertEquals("test", response.rows()[2][0]);
        assertEquals("col1_col2_ft", response.rows()[2][1]);
        assertEquals("fulltext", response.rows()[2][2]);
        assertThat((List<String>) response.rows()[2][3], contains("col1", "col2"));
        assertEquals("analyzer=english", response.rows()[2][4]);

        assertEquals("test", response.rows()[3][0]);
        assertEquals("col3", response.rows()[3][1]);
        assertEquals("fulltext", response.rows()[3][2]);
        assertThat((List<String>) response.rows()[3][3], contains("col3"));
        assertEquals("analyzer=standard", response.rows()[3][4]);
    }*/

    @Test
    public void testGlobalAggregation() throws Exception {
        execute("select max(ordinal_position) from information_schema.columns");
        assertEquals(1, response.rowCount());

        short max_ordinal = 99;
        assertEquals(max_ordinal, response.rows()[0][0]);

        execute("create table t1 (id integer, col1 string)");
        ensureGreen();
        execute("select max(ordinal_position) from information_schema.columns where schema_name='doc'");
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
                "sum(number_of_shards) from information_schema.tables where schema_name='doc'");
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
                "sum(number_of_shards) from information_schema.tables where schema_name='doc' and table_name != 't1'");
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
        assertEquals(22L, response.rows()[0][0]);
    }

    @Test
    public void testGlobalCountDistinct() throws Exception {
        execute("create table t3 (id integer, col1 string)");
        ensureGreen();
        execute("select count(distinct schema_name) from information_schema.tables order by count(distinct schema_name)");
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
        assertEquals("stuff", response.rows()[0][0]);
        short ordinal_position = 1;
        assertEquals(ordinal_position++, response.rows()[0][1]);

        assertEquals("stuff['first_name']", response.rows()[1][0]);
        assertEquals(ordinal_position++, response.rows()[1][1]);

        assertEquals("stuff['last_name']", response.rows()[2][0]);
        assertEquals(ordinal_position++, response.rows()[2][1]);

        assertEquals("title", response.rows()[3][0]);
        assertEquals(ordinal_position, response.rows()[3][1]);

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

        assertEquals("stuff", response.rows()[0][0]);
        ordinal_position = 1;
        assertEquals(ordinal_position++, response.rows()[0][1]);

        assertEquals("stuff['first_name']", response.rows()[1][0]);
        assertEquals(ordinal_position++, response.rows()[1][1]);

        assertEquals("stuff['last_name']", response.rows()[2][0]);
        assertEquals(ordinal_position++, response.rows()[2][1]);

        assertEquals("stuff['middle_name']", response.rows()[3][0]);
        assertEquals(ordinal_position++, response.rows()[3][1]);


        assertEquals("title", response.rows()[4][0]);
        assertEquals(ordinal_position, response.rows()[4][1]);
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
        short ordinal_position = 1;
        assertEquals("stuff", response.rows()[0][0]);
        assertEquals(ordinal_position++, response.rows()[0][1]);

        assertEquals("stuff['first_name']", response.rows()[1][0]);
        assertEquals(ordinal_position++, response.rows()[1][1]);

        assertEquals("stuff['last_name']", response.rows()[2][0]);
        assertEquals(ordinal_position++, response.rows()[2][1]);

        assertEquals("title", response.rows()[3][0]);
        assertEquals(ordinal_position, response.rows()[3][1]);

        execute("insert into t4 (stuff) values (?)", new Object[]{
                new HashMap<String, Object>() {{
                    put("first_name", "Douglas");
                    put("middle_name", "Noel");
                    put("last_name", "Adams");
                }}
        });

        execute("select column_name, ordinal_position from information_schema.columns where table_name='t4'");
        assertEquals(4, response.rowCount());

        ordinal_position = 1;
        assertEquals("stuff", response.rows()[0][0]);
        assertEquals(ordinal_position++, response.rows()[0][1]);

        assertEquals("stuff['first_name']", response.rows()[1][0]);
        assertEquals(ordinal_position++, response.rows()[1][1]);

        assertEquals("stuff['last_name']", response.rows()[2][0]);
        assertEquals(ordinal_position++, response.rows()[2][1]);

        assertEquals("title", response.rows()[3][0]);
        assertEquals(ordinal_position, response.rows()[3][1]);
    }

    @Test
    public void testPartitionedBy() throws Exception {
        execute("create table my_table (id integer, name string) partitioned by (name)");
        execute("create table my_other_table (id integer, name string, content string) " +
                "partitioned by (name, content)");

        execute("select * from information_schema.tables " +
                "where schema_name = 'doc' order by table_name");

        Object[] row1 = new String[] { "name", "content" };
        Object[] row2 = new String[] { "name" };
        assertThat((Object[]) response.rows()[0][5], arrayContaining(row1));
        assertThat((Object[]) response.rows()[1][5], arrayContaining(row2));
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

        execute("select table_name, schema_name, partition_ident, values, number_of_shards, number_of_replicas "+
                "from information_schema.table_partitions order by table_name, partition_ident");
        assertEquals(3, response.rowCount());

        Object[] row1 = new Object[] { "my_table", "doc", "04132", ImmutableMap.of("par", 1), 5, "1"};
        Object[] row2 = new Object[] { "my_table", "doc", "04134", ImmutableMap.of("par", 2), 5, "1"};
        Object[] row3 = new Object[] { "my_table", "doc", "04136", ImmutableMap.of("par", 3), 5, "1"};

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
        execute("select table_name, partition_ident, values, number_of_shards, number_of_replicas "+
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

        Object[] row1 = new Object[] { "my_table", "doc", "08132132c5p0", ImmutableMap.of("par", 1, "par_str", "bar"), 5, "1"};
        Object[] row2 = new Object[] { "my_table", "doc", "08132136dtng", ImmutableMap.of("par", 1, "par_str", "foo"), 5, "1"};
        Object[] row3 = new Object[] { "my_table", "doc", "08134132c5p0", ImmutableMap.of("par", 2, "par_str", "bar"), 5, "1"};
        Object[] row4 = new Object[] { "my_table", "doc", "08134136dtng", ImmutableMap.of("par", 2, "par_str", "foo"), 5, "1"};
        Object[] row5 = new Object[] { "my_table", "doc", "081341b1edi6c", ImmutableMap.of("par", 2, "par_str", "asdf"), 4, "1"};

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
                        1, new MapBuilder<String, Object>().put("date","1970-01-01").map(),
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
        assertThat((String) response.rows()[0][0], is("any1"));
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
        Object[] argsInsert = new Object[] {
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
        execute("select distinct schema_name from information_schema.tables " +
                "where schema_name ~ '[a-z]+o[a-z]' order by schema_name");
        assertThat(response.rowCount(), is(2L));
        assertThat((String)response.rows()[0][0], is("blob"));
        assertThat((String) response.rows()[1][0], is("doc"));
    }

    @Test
    public void testSelectSchemata() throws Exception {
        execute("select * from information_schema.schemata order by schema_name asc");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.getColumn(response.rows(), 0), is(Matchers.<Object>arrayContaining("blob", "doc", "information_schema", "pg_catalog", "sys")));

        execute("create table t1 (col string) with (number_of_replicas=0)");
        ensureGreen();

        execute("select * from information_schema.schemata order by schema_name asc");
        assertThat(response.rowCount(), is(5L));
        assertThat(TestingHelpers.getColumn(response.rows(), 0), is(Matchers.<Object>arrayContaining("blob", "doc", "information_schema", "pg_catalog", "sys")));
    }

    @Test
    public void testOrphanedPartition() throws Exception {
        prepareCreate(".partitioned.foo.04138").execute().get();
        execute("select table_name from information_schema.tables where schema_name='doc'");
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
        assertThat((String) response.rows()[0][1], is("Numeric data types"));
    }
}
