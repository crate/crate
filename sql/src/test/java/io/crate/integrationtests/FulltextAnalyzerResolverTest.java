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
import io.crate.action.sql.SQLActionException;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.crate.testing.SettingMatcher.hasEntry;
import static io.crate.testing.SettingMatcher.hasKey;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class FulltextAnalyzerResolverTest extends SQLTransportIntegrationTest {

    private static FulltextAnalyzerResolver fulltextAnalyzerResolver;

    @Before
    public void AnalyzerServiceSetup() {
        fulltextAnalyzerResolver = internalCluster().getInstance(FulltextAnalyzerResolver.class);
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
        return response.getState().metaData().persistentSettings();
    }

    @Test
    public void resolveSimpleAnalyzerSettings() throws Exception {
        execute("CREATE ANALYZER a1 (tokenizer lowercase)");
        Settings fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a1");
        assertThat(fullAnalyzerSettings.size(), is(2));
        assertThat(
            fullAnalyzerSettings,
            hasEntry("index.analysis.analyzer.a1.type", "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry("index.analysis.analyzer.a1.tokenizer", "lowercase")
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
            hasEntry("index.analysis.analyzer.a2.type", "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry("index.analysis.analyzer.a2.tokenizer", "a2_tok2")
        );
        assertThat(
            fullAnalyzerSettings,
            allOf(
                hasEntry("index.analysis.tokenizer.a2_tok2.type", "ngram"),
                hasEntry("index.analysis.tokenizer.a2_tok2.min_ngram", "2"),
                hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.0", "letter"),
                hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.1", "digits")
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
            hasEntry("index.analysis.analyzer.a3.type", "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry("index.analysis.analyzer.a3.tokenizer", "lowercase")
        );
        assertThat(
            fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a3.char_filter"),
            arrayContainingInAnyOrder("html_strip", "a3_my_mapping")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry("index.analysis.char_filter.a3_my_mapping.type", "mapping")
        );
        assertThat(
            fullAnalyzerSettings.getAsArray("index.analysis.char_filter.a3_my_mapping" +
                                            ".mappings"),
            arrayContainingInAnyOrder("ph=>f", "ß=>ss", "ö=>oe")
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
            hasEntry("index.analysis.analyzer.a4.type", "german")
        );
        assertThat(
            fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a4.stop_words"),
            arrayContainingInAnyOrder("der", "die", "das")
        );

        // extend analyzer who extends builtin analyzer (chain can be longer than 1)
        execute("CREATE ANALYZER a4e EXTENDS " +
                "a4 WITH (" +
                "   \"stop_words\"=['der', 'die', 'das', 'wer', 'wie', 'was']" +
                ")");
        fullAnalyzerSettings = fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings("a4e");
        assertThat(
            fullAnalyzerSettings,
            hasEntry("index.analysis.analyzer.a4e.type", "german")
        );
        assertThat(
            fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a4e.stop_words"),
            arrayContainingInAnyOrder("der", "die", "das", "wer", "wie", "was")
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
                hasEntry("index.analysis.filter.builtin_filter_ngram.type", "ngram"),
                hasEntry("index.analysis.filter.builtin_filter_ngram.min_gram", "1")
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
            hasEntry("index.analysis.analyzer.a5.type", "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry("index.analysis.analyzer.a5.tokenizer", "whitespace")
        );
        assertThat(
            fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a5.filter"),
            arrayContainingInAnyOrder("lowercase", "a5_germanstemmer")
        );
        assertThat(
            fullAnalyzerSettings,
            allOf(
                hasEntry("index.analysis.filter.a5_germanstemmer.type", "stemmer"),
                hasEntry("index.analysis.filter.a5_germanstemmer.language", "german")
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
            hasEntry("index.analysis.analyzer.a5e.type", "custom")
        );
        assertThat(
            fullAnalyzerSettings,
            hasEntry("index.analysis.analyzer.a5e.tokenizer", "letter")
        );
        assertThat(
            fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a5e.filter"),
            arrayContainingInAnyOrder("lowercase", "a5_germanstemmer")
        );
        assertThat(
            fullAnalyzerSettings,
            allOf(
                hasEntry("index.analysis.filter.a5_germanstemmer.type", "stemmer"),
                hasEntry("index.analysis.filter.a5_germanstemmer.language", "german")
            )
        );
        assertThat(
            fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a5e.char_filter"),
            arrayContainingInAnyOrder("html_strip", "a5e_mymapping")
        );
    }

    @Test
    public void testBuiltInAnalyzers() throws Exception {
        List<String> analyzers = new ArrayList<>(fulltextAnalyzerResolver.getBuiltInAnalyzers());
        Collections.sort(analyzers);
        assertThat(Joiner.on(", ").join(analyzers),
            is("arabic, armenian, basque, brazilian, bulgarian, catalan, chinese, cjk, " +
               "czech, danish, default, dutch, english, fingerprint, finnish, french, " +
               "galician, german, greek, hindi, hungarian, indonesian, irish, " +
               "italian, keyword, latvian, lithuanian, norwegian, pattern, persian, portuguese, " +
               "romanian, russian, simple, snowball, sorani, spanish, standard, " +
               "standard_html_strip, stop, swedish, thai, turkish, whitespace"));
    }

    @Test
    public void testBuiltInTokenizers() throws Exception {
        List<String> tokenizers = new ArrayList<>(fulltextAnalyzerResolver.getBuiltInTokenizers());
        Collections.sort(tokenizers);
        assertThat(Joiner.on(", ").join(tokenizers),
            is("PathHierarchy, classic, edgeNGram, edge_ngram, keyword, letter, lowercase, " +
               "nGram, ngram, path_hierarchy, pattern, simple_pattern, simple_pattern_split, standard, thai, " +
               "uax_url_email, whitespace"));
    }

    @Test
    public void testBuiltInTokenFilters() throws Exception {
        List<String> tokenFilters = new ArrayList<>(fulltextAnalyzerResolver.getBuiltInTokenFilters());
        Collections.sort(tokenFilters);
        assertThat(Joiner.on(", ").join(tokenFilters),
            is("apostrophe, arabic_normalization, arabic_stem, asciifolding, brazilian_stem, " +
               "cjk_bigram, cjk_width, classic, common_grams, czech_stem, decimal_digit, " +
               "delimited_payload_filter, dictionary_decompounder, dutch_stem, " +
               "edgeNGram, edge_ngram, elision, fingerprint, flatten_graph, french_stem, german_normalization, " +
               "german_stem, hindi_normalization, hunspell, " +
               "hyphenation_decompounder, indic_normalization, keep, keep_types, " +
               "keyword_marker, " +
               "kstem, length, limit, lowercase, min_hash, nGram, ngram, pattern_capture, " +
               "pattern_replace, persian_normalization, porter_stem, reverse, " +
               "russian_stem, scandinavian_folding, scandinavian_normalization, serbian_normalization, " +
               "shingle, snowball, sorani_normalization, standard, stemmer, stemmer_override, " +
               "stop, synonym, trim, truncate, unique, uppercase, word_delimiter, word_delimiter_graph"));
    }

    @Test
    public void testBuiltInCharFilters() throws Exception {
        List<String> charFilters = new ArrayList<>(fulltextAnalyzerResolver.getBuiltInCharFilters());
        Collections.sort(charFilters);
        assertThat(Joiner.on(", ").join(charFilters),
            is("html_strip, mapping, pattern_replace"));
    }

    @Test
    @UseJdbc(0)
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
                hasKey("crate.analysis.custom.analyzer.a7"),
                hasKey("crate.analysis.custom.tokenizer.a7_mytok"),
                hasKey("crate.analysis.custom.char_filter.a7_mypattern"),
                hasKey("crate.analysis.custom.filter.a7_myshingle"),
                hasKey("crate.analysis.custom.filter.a7_my_stemmer")
            )
        );
        Settings analyzerSettings = FulltextAnalyzerResolver.decodeSettings(settings.get("crate.analysis.custom.analyzer.a7"));
        assertThat(
            analyzerSettings.getAsArray("index.analysis.analyzer.a7.char_filter"),
            arrayContainingInAnyOrder("a7_mypattern", "html_strip")
        );
        assertThat(
            analyzerSettings.getAsArray("index.analysis.analyzer.a7.filter"),
            arrayContainingInAnyOrder("a7_myshingle", "lowercase", "a7_my_stemmer")
        );
        assertThat(
            analyzerSettings,
            hasEntry("index.analysis.analyzer.a7.tokenizer", "a7_mytok")
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
                hasKey("crate.analysis.custom.analyzer.a8"),
                hasKey("crate.analysis.custom.tokenizer.a7_mytok")
            )
        );
        Settings extendedAnalyzerSettings = FulltextAnalyzerResolver.decodeSettings(extendedSettings.get("crate.analysis.custom.analyzer.a8"));
        assertThat(
            extendedAnalyzerSettings,
            hasEntry("index.analysis.analyzer.a8.type", "custom")
        );
        assertThat(
            extendedAnalyzerSettings,
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
    public void reuseExistingTokenizer() {
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
        } catch (SQLActionException e) {
            assertThat(e.getMessage(), containsString("Non-existing tokenizer 'a9tok'"));
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
                hasKey("crate.analysis.custom.analyzer.a11"),
                hasKey("crate.analysis.custom.filter.a11_mystop")
            )
        );
        Settings analyzerSettings = FulltextAnalyzerResolver.decodeSettings(settings.get("crate.analysis.custom.analyzer.a11"));
        Settings tokenFilterSettings = FulltextAnalyzerResolver.decodeSettings(settings.get("crate" +
                                                                                            ".analysis.custom.filter.a11_mystop"));
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
