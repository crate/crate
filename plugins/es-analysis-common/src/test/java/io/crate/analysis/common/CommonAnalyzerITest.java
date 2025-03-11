/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.crate.analysis.common;


import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.testing.TestingHelpers;

public class CommonAnalyzerITest extends IntegTestCase {

    @After
    public void dropCustomAnalyzers() {
        for (String analyzer : List.of(
            "myanalyzer", "myotheranalyzer", "shingle_default", "comma_separation_analyzer")) {
            try {
                execute("DROP ANALYZER " + analyzer);
            } catch (Exception e) {
                // pass, exception may raise cause different
                // custom analyzers are used in different tests
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(CommonAnalysisPlugin.class);
        return plugins;
    }

    @Test
    public void test_select_from_information_schema_with_custom_analyzer() {
        execute("create table quotes (" +
                "id integer primary key, " +
                "quote string index off, " +
                "__quote_info int, " +
                "index quote_fulltext using fulltext(quote) with (analyzer='snowball')" +
                ") clustered by (id) into 3 shards with (number_of_replicas=0)");

        execute("select table_name, number_of_shards, number_of_replicas, clustered_by from " +
                "information_schema.tables " +
                "where table_name='quotes'");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("quotes| 3| 0| id\n");

        execute("select * from information_schema.columns where table_name='quotes'");
        assertThat(response.rowCount()).isEqualTo(3L);

        execute("select * from information_schema.table_constraints where table_schema = ? and table_name='quotes'",
            new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(2L); // 1 PK + 1 NOT NULL derived from the PK (not-null is a display=only constraint).

        execute("select table_name from information_schema.columns where table_schema = ? and table_name='quotes' " +
                "and column_name='__quote_info'", new Object[]{sqlExecutor.getCurrentSchema()});
        assertThat(response.rowCount()).isEqualTo(1L);

        execute("select * from information_schema.routines");
        assertThat(response.rowCount()).isEqualTo(124L);
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
        execute("SELECT routine_name, routine_type, routine_definition from INFORMATION_SCHEMA.routines " +
                "where routine_name = 'myanalyzer' " +
                "or routine_name = 'myotheranalyzer' " +
                "and routine_type = 'ANALYZER' " +
                "order by routine_name asc");
        assertThat(response.rowCount()).isEqualTo(2L);
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo(
            """
                myanalyzer| ANALYZER| {"filter":["myanalyzer_mytokenfilter","kstem"],"tokenizer":"whitespace","type":"custom"}
                myotheranalyzer| ANALYZER| {"stopwords":["der","die","das"],"type":"german"}
                """);

        assertThat(response.rows()[0][0]).isEqualTo("myanalyzer");
        assertThat(response.rows()[0][1]).isEqualTo("ANALYZER");
        assertThat(response.rows()[1][0]).isEqualTo("myotheranalyzer");
        assertThat(response.rows()[1][1]).isEqualTo("ANALYZER");
    }

    @Test
    public void testShingleFilterWithGraphOutput() throws Exception {
        execute("""
            CREATE ANALYZER shingle_default (
                TOKENIZER "standard",
                TOKEN_FILTERS (
                    "lowercase",
                    "stop",
                    "shingle"
                )
            );
            """);
        execute("create table t2  (c1 TEXT INDEX USING fulltext WITH (analyzer='shingle_default'));");
        ensureGreen();
        execute("insert into t2 (c1) values ('This sentence only has Humans in the text.');");
    }

    @Test
    public void test_add_column_with_custom_analyzer() {
        execute("""
            CREATE ANALYZER comma_separation_analyzer EXTENDS "standard" with (
                TOKENIZER mypattern WITH (
                   type = 'pattern',
                   pattern = ',\\\\s'
                )
                , TOKEN_FILTERS (lowercase)
            )
            """);
        execute("CREATE TABLE tbl (keywords TEXT)");

        execute("""
            ALTER TABLE tbl
            ADD COLUMN keywords_analyzed TEXT INDEX USING FULLTEXT WITH (analyzer = 'comma_separation_analyzer')
            """);
        execute("""
            INSERT INTO tbl (keywords, keywords_analyzed) VALUES(
            'some articles',
            'Humans, Computational Biology, Reactive Oxygen Species, Superoxide Dismutase, Rare Diseases, Gene Ontology, Oxidative Stress')
            """);
        execute("REFRESH TABLE tbl");
        execute("""
            SELECT keywords FROM tbl
            WHERE MATCH(keywords_analyzed, 'biology')
            """);
        assertThat(response).hasRows("some articles");
    }

    @Test
    public void test_add_column_with_custom_analyzer_to_partitioned_table() {
        execute("""
            CREATE ANALYZER comma_separation_analyzer EXTENDS "standard" with (
                TOKENIZER mypattern WITH (
                   type = 'pattern',
                   pattern = ',\\\\s'
                )
                , TOKEN_FILTERS (lowercase)
            )
            """);
        execute("""
            CREATE TABLE tbl_parted (id int primary key, ts timestamp primary key, keywords TEXT)
            PARTITIONED BY (ts)
            CLUSTERED INTO 1 SHARDS
            """);
        // Create 2 partitions
        execute("INSERT INTO tbl_parted (id, ts) VALUES(1, now())");
        execute("INSERT INTO tbl_parted (id, ts) VALUES(2, now() + INTERVAL '10 day')");
        execute("REFRESH TABLE tbl_parted");
        execute("""
            ALTER TABLE tbl_parted
            ADD COLUMN keywords_analyzed TEXT INDEX USING FULLTEXT WITH (analyzer = 'comma_separation_analyzer')
            """);
        execute("""
            INSERT INTO tbl_parted (id, ts, keywords_analyzed) VALUES(
            3,
            now(),
            'Humans, Computational Biology, Reactive Oxygen Species, Superoxide Dismutase, Rare Diseases, Gene Ontology, Oxidative Stress')
            """);
        execute("REFRESH TABLE tbl_parted");
        execute("""
            SELECT id FROM tbl_parted
            WHERE MATCH(keywords_analyzed, 'biology')
            """);
        assertThat(response).hasRows("3");
    }
}
