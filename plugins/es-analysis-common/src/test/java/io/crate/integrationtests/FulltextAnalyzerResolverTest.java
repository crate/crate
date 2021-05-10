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

import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.ANALYZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.CHAR_FILTER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKENIZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKEN_FILTER;
import static io.crate.protocols.postgres.PGErrorStatus.INTERNAL_ERROR;
import static io.crate.testing.Asserts.assertThrows;
import static io.crate.testing.SQLErrorMatcher.isSQLError;
import static io.crate.testing.SettingMatcher.hasEntry;
import static io.crate.testing.SettingMatcher.hasKey;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.testing.SQLResponse;

public class FulltextAnalyzerResolverTest extends SQLIntegrationTestCase {

    private static FulltextAnalyzerResolver fulltextAnalyzerResolver;

    @Before
    public void AnalyzerServiceSetup() {
        fulltextAnalyzerResolver = internalCluster().getInstance(FulltextAnalyzerResolver.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(CommonAnalysisPlugin.class);
        return plugins;
    }

    @AfterClass
    public static void tearDownClass() {
        synchronized (FulltextAnalyzerResolverTest.class) {
            fulltextAnalyzerResolver = null;
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        Map<String, Object> settingsToRemove = new HashMap<>();
        getPersistentClusterSettings().keySet().stream()
            .filter(s -> s.startsWith("crate"))
            .forEach(s -> settingsToRemove.put(s, null));
        if (!settingsToRemove.isEmpty()) {
            client().admin().cluster().prepareUpdateSettings()
                .setPersistentSettings(settingsToRemove)
                .setTransientSettings(settingsToRemove).execute().actionGet();
        }
        super.tearDown();
    }

    public Settings getPersistentClusterSettings() {
        ClusterStateResponse response = client().admin().cluster().prepareState().execute().actionGet();
        return response.getState().metadata().persistentSettings();
    }

    @Test
    public void resolveSimpleAnalyzerSettings() throws Exception {
        execute("CREATE ANALYZER a1 (tokenizer lowercase)");
        Settings fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a1");
        assertThat(fullAnalyzerSettings.size(), is(2));
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a1", "type"), "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a1", TOKENIZER.getName()), "lowercase")
        );
    }

    @Test
    public void resolveAnalyzerWithCustomTokenizer() throws Exception {
        execute("CREATE ANALYZER a2" +
                "(" +
                "   tokenizer tok2 with (" +
                "       type='ngram'," +
                "       \"min_ngram\"=2," +
                "       \"token_chars\"=['letter', 'digits']" +
                "   )" +
                ")");
        Settings fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a2");
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a2", "type"), "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a2", TOKENIZER.getName()), "a2_tok2")
        );
        assertThat(
            fullAnalyzerSettings,
            allOf(
                hasEntry(TOKENIZER.buildSettingChildName("a2_tok2", "type"), "ngram"),
                hasEntry(TOKENIZER.buildSettingChildName("a2_tok2", "min_ngram"), "2"),
                hasEntry(TOKENIZER.buildSettingChildName("a2_tok2", "token_chars"), "[letter, digits]")
            )
        );
    }

    @Test
    public void resolveAnalyzerWithCharFilters() throws Exception {
        execute("CREATE ANALYZER a3" +
                "(" +
                "   tokenizer lowercase," +
                "   char_filters (" +
                "       \"html_strip\"," +
                "       my_mapping WITH (" +
                "           type='mapping'," +
                "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
                "       )" +
                "   )" +
                ")");
        Settings fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a3");
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a3", "type"), "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a3", TOKENIZER.getName()), "lowercase")
        );
        assertThat(
            fullAnalyzerSettings.getAsList(ANALYZER.buildSettingChildName("a3", CHAR_FILTER.getName())),
            containsInAnyOrder("html_strip", "a3_my_mapping")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry(CHAR_FILTER.buildSettingChildName("a3_my_mapping","type"), "mapping")
        );
        assertThat(
            fullAnalyzerSettings.getAsList(CHAR_FILTER.buildSettingChildName("a3_my_mapping", "mappings")),
            containsInAnyOrder("ph=>f", "ß=>ss", "ö=>oe")
        );
        execute("CREATE TABLE t1(content " +
                "string index using fulltext with (analyzer='a3'))");
    }

    @Test
    public void resolveAnalyzerExtendingBuiltin() throws Exception {
        execute("CREATE ANALYZER a4 EXTENDS " +
                "german WITH (" +
                "   \"stop_words\"=['der', 'die', 'das']" +
                ")");
        Settings fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a4");
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a4", "type"), "german")
        );
        assertThat(
            fullAnalyzerSettings.getAsList(ANALYZER.buildSettingChildName("a4", "stop_words")),
            containsInAnyOrder("der", "die", "das")
        );

        // extend analyzer who extends builtin analyzer (chain can be longer than 1)
        execute("CREATE ANALYZER a4e EXTENDS " +
                "a4 WITH (" +
                "   \"stop_words\"=['der', 'die', 'das', 'wer', 'wie', 'was']" +
                ")");
        fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a4e");
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a4e", "type"), "german")
        );
        assertThat(
            fullAnalyzerSettings.getAsList(ANALYZER.buildSettingChildName("a4e", "stop_words")),
            containsInAnyOrder("der", "die", "das", "wer", "wie", "was")
        );
    }

    @Test
    public void resolveAnalyzerBuiltinTokenFilter() throws Exception {
        execute("CREATE ANALYZER builtin_filter (" +
                "   tokenizer whitespace," +
                "   token_filters (" +
                "       ngram WITH (" +
                "           min_gram=1" +
                "       )" +
                "   )" +
                ")");
        Settings fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("builtin_filter");
        assertThat(
            fullAnalyzerSettings,
            allOf(
                hasEntry(TOKEN_FILTER.buildSettingChildName("builtin_filter_ngram", "type"), "ngram"),
                hasEntry(TOKEN_FILTER.buildSettingChildName("builtin_filter_ngram", "min_gram"), "1")
            )
        );
    }

    @Test
    public void resolveAnalyzerExtendingCustom() throws Exception {
        execute("CREATE ANALYZER a5 (" +
                "   tokenizer whitespace," +
                "   token_filters (" +
                "       lowercase," +
                "       germanstemmer WITH (" +
                "           type='stemmer'," +
                "           language='german'" +
                "       )" +
                "   )" +
                ")");
        Settings fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a5");
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a5", "type"), "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a5", TOKENIZER.getName()), "whitespace")
        );
        assertThat(
            fullAnalyzerSettings.getAsList(ANALYZER.buildSettingChildName("a5", TOKEN_FILTER.getName())),
            containsInAnyOrder("lowercase", "a5_germanstemmer")
        );
        assertThat(
            fullAnalyzerSettings,
            allOf(
                hasEntry(TOKEN_FILTER.buildSettingChildName("a5_germanstemmer", "type"), "stemmer"),
                hasEntry(TOKEN_FILTER.buildSettingChildName("a5_germanstemmer", "language"), "german")
            )
        );

        execute("CREATE ANALYZER a5e EXTENDS a5 (" +
                "   tokenizer letter," +
                "   char_filters (" +
                "       \"html_strip\"," +
                "       mymapping WITH (" +
                "           type='mapping'," +
                "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
                "       )" +
                "   )" +
                ")");

        fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a5e");
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a5e", "type"), "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a5e", TOKENIZER.getName()), "letter")
        );
        assertThat(
            fullAnalyzerSettings.getAsList(ANALYZER.buildSettingChildName("a5e", TOKEN_FILTER.getName())),
            containsInAnyOrder("lowercase", "a5_germanstemmer")
        );
        assertThat(
            fullAnalyzerSettings,
            allOf(
                hasEntry(TOKEN_FILTER.buildSettingChildName("a5_germanstemmer", "type"), "stemmer"),
                hasEntry(TOKEN_FILTER.buildSettingChildName("a5_germanstemmer", "language"), "german")
            )
        );
        assertThat(
            fullAnalyzerSettings.getAsList(ANALYZER.buildSettingChildName("a5e", CHAR_FILTER.getName())),
            containsInAnyOrder("html_strip", "a5e_mymapping")
        );
    }

    @Test
    public void testBuiltInAnalyzers() throws Exception {
        List<String> analyzers = new ArrayList<>(fulltextAnalyzerResolver.getBuiltInAnalyzers());
        Collections.sort(analyzers);
        assertThat(String.join(", ", analyzers),
            is("arabic, armenian, basque, bengali, brazilian, bulgarian, catalan, chinese, cjk, " +
               "czech, danish, default, dutch, english, fingerprint, finnish, french, " +
               "galician, german, greek, hindi, hungarian, indonesian, irish, " +
               "italian, keyword, latvian, lithuanian, norwegian, pattern, persian, portuguese, " +
               "romanian, russian, simple, snowball, sorani, spanish, standard, " +
               "standard_html_strip, stop, swedish, thai, turkish, whitespace"));
    }

    @Test
    public void testBuiltInTokenizers() throws Exception {
        List<String> tokenizers = new ArrayList<>(fulltextAnalyzerResolver.getBuiltInTokenizers());
        assertThat(tokenizers, containsInAnyOrder(
            "PathHierarchy",
            "char_group",
            "classic",
            "edge_ngram",
            "keyword",
            "letter",
            "lowercase",
            "ngram",
            "path_hierarchy",
            "pattern",
            "simple_pattern",
            "simple_pattern_split",
            "standard",
            "thai",
            "uax_url_email",
            "whitespace"
        ));
    }

    @Test
    public void testBuiltInTokenFilters() throws Exception {
        List<String> tokenFilters = new ArrayList<>(fulltextAnalyzerResolver.getBuiltInTokenFilters());
        Collections.sort(tokenFilters);
        assertThat(String.join(", ", tokenFilters),
            is("apostrophe, arabic_normalization, arabic_stem, asciifolding, bengali_normalization, brazilian_stem, " +
               "cjk_bigram, cjk_width, classic, common_grams, czech_stem, decimal_digit, delimited_payload, " +
               "delimited_payload_filter, dictionary_decompounder, dutch_stem, " +
               "edge_ngram, elision, fingerprint, flatten_graph, french_stem, german_normalization, " +
               "german_stem, hindi_normalization, hunspell, " +
               "hyphenation_decompounder, indic_normalization, keep, keep_types, " +
               "keyword_marker, " +
               "kstem, length, limit, lowercase, min_hash, multiplexer, ngram, pattern_capture, " +
               "pattern_replace, persian_normalization, porter_stem, remove_duplicates, reverse, " +
               "russian_stem, scandinavian_folding, scandinavian_normalization, serbian_normalization, " +
               "shingle, snowball, sorani_normalization, standard, stemmer, stemmer_override, " +
               "stop, synonym, trim, truncate, unique, uppercase, word_delimiter, word_delimiter_graph"));
    }

    @Test
    public void testBuiltInCharFilters() throws Exception {
        List<String> charFilters = new ArrayList<>(fulltextAnalyzerResolver.getBuiltInCharFilters());
        Collections.sort(charFilters);
        assertThat(String.join(", ", charFilters),
            is("html_strip, mapping, pattern_replace"));
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
                "    type='edge_ngram'," +
                "    \"min_gram\" = 2," +
                "    \"max_gram\" = 5," +
                "    \"token_chars\" = [ 'letter', 'digit' ]" +
                "  )," +
                "  token_filters (" +
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
            settings,
            allOf(
                hasKey(ANALYZER.buildSettingName("a7")),
                hasKey(TOKENIZER.buildSettingName("a7_mytok")),
                hasKey(CHAR_FILTER.buildSettingName("a7_mypattern")),
                hasKey(TOKEN_FILTER.buildSettingName("a7_myshingle")),
                hasKey(TOKEN_FILTER.buildSettingName("a7_my_stemmer"))
            )
        );
        Settings analyzerSettings = FulltextAnalyzerResolver.decodeSettings(settings.get(ANALYZER.buildSettingName("a7")));
        assertThat(
            analyzerSettings.getAsList(ANALYZER.buildSettingChildName("a7", CHAR_FILTER.getName())),
            containsInAnyOrder("a7_mypattern", "html_strip")
        );
        assertThat(
            analyzerSettings.getAsList(ANALYZER.buildSettingChildName("a7", TOKEN_FILTER.getName())),
            containsInAnyOrder("a7_myshingle", "lowercase", "a7_my_stemmer")
        );
        assertThat(
            analyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a7", TOKENIZER.getName()), "a7_mytok")
        );
        execute("CREATE ANALYZER a8 EXTENDS a7 (" +
                "  token_filters (" +
                "    lowercase," +
                "    kstem" +
                "  )" +
                ")");
        Settings extendedSettings = getPersistentClusterSettings();
        assertThat(
            extendedSettings,
            allOf(
                hasKey(ANALYZER.buildSettingName("a8")),
                hasKey(TOKENIZER.buildSettingName("a7_mytok"))
            )
        );
        Settings extendedAnalyzerSettings = FulltextAnalyzerResolver.decodeSettings(extendedSettings.get(ANALYZER.buildSettingName("a8")));
        assertThat(
            extendedAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a8", "type"), "custom")
        );
        assertThat(
            extendedAnalyzerSettings,
            hasEntry(ANALYZER.buildSettingChildName("a8", TOKENIZER.getName()), "a7_mytok")
        );
        assertThat(
            extendedAnalyzerSettings.getAsList(ANALYZER.buildSettingChildName("a8", TOKEN_FILTER.getName())),
            containsInAnyOrder("lowercase", "kstem")
        );
        assertThat(
            extendedAnalyzerSettings.getAsList(ANALYZER.buildSettingChildName("a8", CHAR_FILTER.getName())),
            containsInAnyOrder("a7_mypattern", "html_strip")
        );

    }

    @Test
    public void reuseExistingTokenizer() {
        execute("CREATE ANALYZER a9 (" +
                "  TOKENIZER a9tok WITH (" +
                "    type='ngram'," +
                "    \"token_chars\"=['letter', 'digit']" +
                "  )" +
                ")");
        assertThrows(() -> execute("CREATE ANALYZER a10 (TOKENIZER a9tok)"),
                     isSQLError(endsWith("Non-existing tokenizer 'a9tok'"),
                                INTERNAL_ERROR,
                                BAD_REQUEST,
                                4000));

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
    public void useAnalyzerForIndexSettings() throws Exception {
        execute("CREATE ANALYZER a11 (" +
                "  TOKENIZER standard," +
                "  TOKEN_FILTERS (" +
                "    lowercase," +
                "    mystop WITH (" +
                "      type='stop'," +
                "      stopword=['the', 'over']" +
                "    )" +
                "  )" +
                ")");
        Settings settings = getPersistentClusterSettings();
        assertThat(
            settings,
            allOf(
                hasKey(ANALYZER.buildSettingName("a11")),
                hasKey(TOKEN_FILTER.buildSettingName("a11_mystop"))
            )
        );
        Settings analyzerSettings = FulltextAnalyzerResolver.decodeSettings(settings.get(ANALYZER.buildSettingName("a11")));
        Settings tokenFilterSettings = FulltextAnalyzerResolver.decodeSettings(settings.get(TOKEN_FILTER.buildSettingName("a11_mystop")));
        Settings.Builder builder = Settings.builder();
        builder.put(analyzerSettings);
        builder.put(tokenFilterSettings);

        execute("create table test (" +
                " id integer primary key," +
                " name string," +
                " content string index using fulltext with (analyzer='a11')" +
                ")");
        ensureYellow();
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
