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

package org.cratedb.integrationtests;

import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.SQLResponse;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.collection.IsMapContaining.hasKey;

public class CrateClusterSettingsActionTest extends SQLTransportIntegrationTest  {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public Settings getPersistentClusterSettings() {
        ClusterStateResponse response = client().admin().cluster().prepareState().execute().actionGet();
        return response.getState().metaData().persistentSettings();
    }

    @Test
    public void createSimpleAnalyzer() throws IOException {
        SQLResponse response = execute("CREATE ANALYZER a1 WITH (" +
                "  TOKENIZER standard" +
                ")");
        assertThat(response.duration(), greaterThanOrEqualTo(0L));
        Settings customAnalyzerSettings = getPersistentClusterSettings();

        assertThat(
                customAnalyzerSettings.getAsMap(),
                hasKey("crate.analysis.custom.analyzer.a1")
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(customAnalyzerSettings.get("crate.analysis.custom.analyzer.a1"));
        assertThat(
            analyzerSettings.getAsMap(),
            allOf(
                    hasEntry("index.analysis.analyzer.a1.type", "custom"),
                    hasEntry("index.analysis.analyzer.a1.tokenizer", "standard")
            )
        );


    }

    @Test
    public void createAnalyzerWithCustomTokenizer() throws IOException {
        execute("CREATE ANALYZER a2 WITH (" +
                "  TOKENIZER custom WITH (" +
                "    type='keyword'," +
                "    \"buffer_size\"=1024" +
                "  )" +
                ")");
        Settings settings = getPersistentClusterSettings();
        assertThat(
                settings.getAsMap(),
                hasKey("crate.analysis.custom.analyzer.a2")
        );
        assertThat(
                settings.getAsMap(),
                hasKey("crate.analysis.custom.tokenizer.a2_custom")
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analysis.custom.analyzer.a2"));
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a2.type", "custom")
        );
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a2.tokenizer", "a2_custom")
        );

        Settings tokenizerSettings = AnalyzerService.decodeSettings(settings.get("crate.analysis" +
                ".custom.tokenizer.a2_custom"));
        assertThat(
                tokenizerSettings.getAsMap(),
                hasEntry("index.analysis.tokenizer.a2_custom.type", "keyword")
        );
        assertThat(tokenizerSettings.getAsMap(), hasEntry("index.analysis.tokenizer.a2_custom" +
                ".buffer_size", "1024"));
    }

    @Test
    public void createExtendingCustomAnalyzer() throws IOException {
        execute("CREATE ANALYZER a3 (" +
                "  token_filters (" +
                "    greeklowercase with (" +
                "      type='lowercase'," +
                "      language='greek'" +
                "    )," +
                "    ngram" +
                "  )," +
                "  tokenizer standard" +
                ")");
        Settings settings = getPersistentClusterSettings();
        assertThat(
                settings.getAsMap(),
                allOf(
                        hasKey("crate.analysis.custom.analyzer.a3"),
                        hasKey("crate.analysis.custom.filter.a3_greeklowercase")
                )
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analysis.custom.analyzer.a3"));
        assertThat(
                analyzerSettings.getAsArray("index.analysis.analyzer.a3.filter"),
                arrayContainingInAnyOrder("ngram", "a3_greeklowercase")
        );
        assertThat(analyzerSettings.getAsMap(), hasEntry("index.analysis.analyzer.a3.tokenizer", "standard"));

        execute("CREATE ANALYZER a4 EXTENDS a3 WITH (" +
                "  char_filters WITH (" +
                "    html_strip" +
                "  )," +
                "  tokenizer whitespace" +
                ")");
        Settings extendedSettings = getPersistentClusterSettings();
        assertThat(
                extendedSettings.getAsMap(),
                allOf(
                        hasKey("crate.analysis.custom.analyzer.a3"),
                        hasKey("crate.analysis.custom.analyzer.a4")
                )
        );

        Settings extendedAnalyzerSettings = AnalyzerService.decodeSettings(extendedSettings.get("crate.analysis.custom.analyzer.a4"));
        assertThat(extendedAnalyzerSettings.getAsArray("index.analysis.analyzer.a4.char_filter"), arrayContainingInAnyOrder("html_strip"));
        assertThat(extendedAnalyzerSettings.getAsArray("index.analysis.analyzer.a4.filter"),
                arrayContainingInAnyOrder("ngram", "a3_greeklowercase"));
        assertThat(extendedAnalyzerSettings.getAsMap(), hasEntry("index.analysis.analyzer.a4.tokenizer", "whitespace"));

    }

    @Test
    public void createExtendingBuiltinAnalyzer() throws IOException {
        execute("CREATE ANALYZER a5 EXTENDS stop WITH (" +
                "   stopwords=['foo', 'bar', 'baz']" +
                ")");
        Settings settings = getPersistentClusterSettings();
        assertThat(
                settings.getAsMap(),
                hasKey("crate.analysis.custom.analyzer.a5")
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analysis.custom.analyzer.a5"));
        assertThat(
                analyzerSettings.getAsMap(), hasEntry("index.analysis.analyzer.a5.type", "stop")
        );
        assertThat(analyzerSettings.getAsArray("index.analysis.analyzer.a5.stopwords"), arrayContainingInAnyOrder("foo", "bar", "baz"));
    }


    @Test(expected = SQLParseException.class)
    public void createAnalyzerWithoutTokenizer() throws IOException {
        execute("CREATE ANALYZER a6 WITH (" +
                "  char_filters WITH (" +
                "    \"html_strip\"" +
                "  )," +
                "  token_filters WITH (" +
                "    lowercase" +
                "  )" +
                ")");
    }

    @Test
    public void createAndExtendFullCustomAnalyzer() throws IOException {
        execute("CREATE ANALYZER a7 (" +
                "  char_filters (" +
                "     mypattern WITH (" +
                "       type='pattern_replace'," +
                "      \"pattern\" ='sample(.*)',\n" +
                "      \"replacement\" = 'replacedSample $1'" +
                "     )," +
                "     \"html_strip\"" +
                "  )," +
                "  tokenizer mytok WITH (" +
                "    type='edgeNGram'," +
                "    \"min_gram\" = 2," +
                "    \"max_gram\" = 5," +
                "    \"token_chars\" = [ 'letter', 'digit' ]" +
                "  )," +
                "  token_filters WITH (" +
                "    myshingle WITH (" +
                "      type='shingle'," +
                "      \"output_unigrams\"=false," +
                "      \"max_shingle_size\"=10" +
                "    )," +
                "    lowercase," +
                "    \"my_stemmer\" WITH (" +
                "      type='stemmer'," +
                "      language='german'" +
                "    )" +
                "  )" +
                ")");
        Settings settings = getPersistentClusterSettings();

        assertThat(
                settings.getAsMap(),
                allOf(
                        hasKey("crate.analysis.custom.analyzer.a7"),
                        hasKey("crate.analysis.custom.tokenizer.a7_mytok"),
                        hasKey("crate.analysis.custom.char_filter.a7_mypattern"),
                        hasKey("crate.analysis.custom.filter.a7_myshingle"),
                        hasKey("crate.analysis.custom.filter.a7_my_stemmer")
                )
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analysis.custom.analyzer.a7"));
        assertThat(
                analyzerSettings.getAsArray("index.analysis.analyzer.a7.char_filter"),
                arrayContainingInAnyOrder("a7_mypattern", "html_strip")
        );
        assertThat(
                analyzerSettings.getAsArray("index.analysis.analyzer.a7.filter"),
                arrayContainingInAnyOrder("a7_myshingle", "lowercase", "a7_my_stemmer")
        );
        assertThat(
                analyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a7.tokenizer", "a7_mytok")
        );
        execute("CREATE ANALYZER a8 EXTENDS a7 WITH (" +
                "  token_filters (" +
                "    lowercase," +
                "    kstem" +
                "  )" +
                ")");
        Settings extendedSettings = getPersistentClusterSettings();
        assertThat(
            extendedSettings.getAsMap(),
            allOf(
                    hasKey("crate.analysis.custom.analyzer.a8"),
                    hasKey("crate.analysis.custom.tokenizer.a7_mytok")
            )
        );
        Settings extendedAnalyzerSettings = AnalyzerService.decodeSettings(extendedSettings.get("crate.analysis.custom.analyzer.a8"));
        assertThat(
                extendedAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a8.type", "custom")
        );
        assertThat(
                extendedAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a8.tokenizer", "a7_mytok")
        );
        assertThat(
                extendedAnalyzerSettings.getAsArray("index.analysis.analyzer.a8.filter"),
                arrayContainingInAnyOrder("lowercase", "kstem")
        );
        assertThat(
                extendedAnalyzerSettings.getAsArray("index.analysis.analyzer.a8.char_filter"),
                arrayContainingInAnyOrder("a7_mypattern", "html_strip")
        );

    }

    @Test
    public void reuseExistingTokenizer() throws StandardException, IOException, InterruptedException {

        execute("CREATE ANALYZER a9 (" +
                "  TOKENIZER a9tok WITH (" +
                "    type='nGram'," +
                "    \"token_chars\"=['letter', 'digit']" +
                "  )" +
                ")");
        try {
            execute("CREATE ANALYZER a10 (" +
                    "  TOKENIZER a9tok" +
                    ")");
            fail("Reusing existing tokenizer worked");
        } catch (SQLParseException e) {
            assertThat(e.getMessage(), is("Non-existing tokenizer 'a9tok'"));
        }
        /*
         * NOT SUPPORTED UNTIL A CONSISTENT SOLUTION IS FOUND
         * FOR IMPLICITLY CREATING TOKENIZERS ETC. WITHIN ANALYZER-DEFINITIONS

        Settings settings = getPersistentClusterSettings();
        Settings a10Settings = AnalyzerService.decodeSettings(settings.get("crate.analysis.custom.analyzer.a10"));
        assertThat(
                a10Settings.getAsMap(),
                hasEntry("index.analysis.analyzer.a10.tokenizer", "a9tok")
        );
        */
    }

    @Test
    public void useAnalyzerForIndexSettings() throws StandardException, IOException {
        execute("CREATE ANALYZER a11 (" +
                "  TOKENIZER standard," +
                "  TOKEN_FILTERS WITH (" +
                "    lowercase," +
                "    mystop WITH (" +
                "      type='stop'," +
                "      stopwords=['the', 'over']" +
                "    )" +
                "  )" +
                ")");
        Settings settings = getPersistentClusterSettings();
        assertThat(
            settings.getAsMap(),
            allOf(
                    hasKey("crate.analysis.custom.analyzer.a11"),
                    hasKey("crate.analysis.custom.filter.a11_mystop")
            )
        );
        Settings analyzerSettings = AnalyzerService.decodeSettings(settings.get("crate.analysis.custom.analyzer.a11"));
        Settings tokenFilterSettings = AnalyzerService.decodeSettings(settings.get("crate" +
                ".analysis.custom.filter.a11_mystop"));
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        builder.put(analyzerSettings);
        builder.put(tokenFilterSettings);

        Client client = client();
        execute("create table test (" +
                " id integer primary key," +
                " name string," +
                " content string index using fulltext with (analyzer='a11')" +
                ")");
        ensureGreen();
        execute("insert into test (id, name, content) values (?, ?, ?)", new Object[]{
                1, "phrase", "The quick brown fox jumps over the lazy dog."
        });
        execute("insert into test (id, name, content) values (?, ?, ?)", new Object[]{
                2, "another phrase", "Don't panic!"
        });
        refresh();
        SQLResponse response = execute("select id from test where match(content, 'brown jump')");
        assertEquals(1L, response.rowCount());
        assertEquals(1, response.rows()[0][0]);
    }
}
