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

package org.cratedb.service;

import com.google.common.base.Joiner;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.action.sql.analyzer.AnalyzerService;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.common.settings.Settings;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.*;

public class AnalyzerServiceTest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static AnalyzerService analyzerService;

    @Before
    public void AnalyzerServiceSetup() {
        analyzerService = cluster().getInstance(AnalyzerService.class);
    }

    @AfterClass
    public static void tearDownClass() {
        synchronized (AnalyzerServiceTest.class) {
            analyzerService = null;
        }
    }

    @Test
    public void resolveSimpleAnalyzerSettings() throws StandardException {
        execute("CREATE ANALYZER a1 WITH (tokenizer lowercase)");
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a1");
        assertThat(fullAnalyzerSettings.getAsMap().size(), is(2));
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a1.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a1.tokenizer", "lowercase")
        );
    }

    @Test
    public void resolveAnalyzerWithCustomTokenizer() throws StandardException {
        execute("CREATE ANALYZER a2 WITH" +
                "(" +
                "   tokenizer tok2 with (" +
                "       type='ngram'," +
                "       \"min_ngram\"=2," +
                "       \"token_chars\"=['letter', 'digits']" +
                "   )" +
                ")");
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a2");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a2.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a2.tokenizer", "a2_tok2")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                allOf(
                        hasEntry("index.analysis.tokenizer.a2_tok2.type", "ngram"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.min_ngram", "2"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.0", "letter"),
                        hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.1", "digits")
                )
        );
    }

    @Test
    public void resolveAnalyzerWithCharFilters() throws StandardException {
        execute("CREATE ANALYZER a3 WITH" +
                "(" +
                "   tokenizer lowercase," +
                "   char_filters WITH (" +
                "       \"html_strip\"," +
                "       my_mapping WITH (" +
                "           type='mapping'," +
                "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
                "       )" +
                "   )" +
                ")");
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a3");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a3.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a3.tokenizer", "lowercase")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a3.char_filter"),
                arrayContainingInAnyOrder("html_strip", "a3_my_mapping")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
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
    public void resolveAnalyzerExtendingBuiltin() throws StandardException {
        execute("CREATE ANALYZER a4 EXTENDS " +
                "german WITH (" +
                "   \"stop_words\"=['der', 'die', 'das']" +
                ")");
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a4");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
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
        fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a4e");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a4e.type", "german")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a4e.stop_words"),
                arrayContainingInAnyOrder("der", "die", "das", "wer", "wie", "was")
        );
    }

    @Test
    public void resolveAnalyzerExtendingCustom() throws StandardException {
        execute("CREATE ANALYZER a5 WITH (" +
                "   tokenizer whitespace," +
                "   token_filters (" +
                "       lowercase," +
                "       germanstemmer WITH (" +
                "           type='stemmer'," +
                "           language='german'" +
                "       )" +
                "   )" +
                ")");
        Settings fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a5");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a5.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a5.tokenizer", "whitespace")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a5.filter"),
                arrayContainingInAnyOrder("lowercase", "a5_germanstemmer")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                allOf(
                    hasEntry("index.analysis.filter.a5_germanstemmer.type", "stemmer"),
                    hasEntry("index.analysis.filter.a5_germanstemmer.language", "german")
                )
        );

         execute("CREATE ANALYZER a5e EXTENDS a5" +
                " WITH (" +
                "   tokenizer letter," +
                "   char_filters WITH (" +
                "       \"html_strip\"," +
                "       mymapping WITH (" +
                "           type='mapping'," +
                "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
                "       )" +
                "   )" +
                ")");

        fullAnalyzerSettings = analyzerService.resolveFullCustomAnalyzerSettings("a5e");
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a5e.type", "custom")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
                hasEntry("index.analysis.analyzer.a5e.tokenizer", "letter")
        );
        assertThat(
                fullAnalyzerSettings.getAsArray("index.analysis.analyzer.a5e.filter"),
                arrayContainingInAnyOrder("lowercase", "a5_germanstemmer")
        );
        assertThat(
                fullAnalyzerSettings.getAsMap(),
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
        List<String> analyzers = new ArrayList<>(analyzerService.getBuiltInAnalyzers());
        Collections.sort(analyzers);
        assertThat(Joiner.on(", ").join(analyzers),
                is("arabic, armenian, basque, brazilian, bulgarian, catalan, chinese, cjk, " +
                        "classic, czech, danish, default, dutch, english, finnish, french, " +
                        "galician, german, greek, hindi, hungarian, indonesian, irish, " +
                        "italian, keyword, latvian, norwegian, pattern, persian, portuguese, " +
                        "romanian, russian, simple, snowball, spanish, standard, " +
                        "standard_html_strip, stop, swedish, thai, turkish, whitespace"));
    }

    @Test
    public void testBuiltInTokenizers() throws Exception {
        List<String> tokenizers = new ArrayList<>(analyzerService.getBuiltInTokenizers());
        Collections.sort(tokenizers);
        assertThat(Joiner.on(", ").join(tokenizers),
                is("classic, edgeNGram, edge_ngram, keyword, letter, lowercase, " +
                        "nGram, ngram, path_hierarchy, pattern, standard, " +
                        "uax_url_email, whitespace"));
    }

    @Test
    public void testBuiltInTokenFilters() throws Exception {
        List<String> tokenFilters = new ArrayList<>(analyzerService.getBuiltInTokenFilters());
        Collections.sort(tokenFilters);
        assertThat(Joiner.on(", ").join(tokenFilters),
                is("arabic_normalization, arabic_stem, asciifolding, brazilian_stem, " +
                        "cjk_bigram, cjk_width, classic, common_grams, czech_stem, " +
                        "delimited_payload_filter, dictionary_decompounder, dutch_stem, " +
                        "edgeNGram, edge_ngram, elision, french_stem, german_stem, hunspell, " +
                        "hyphenation_decompounder, keep, keyword_marker, keyword_repeat, " +
                        "kstem, length, limit, lowercase, nGram, ngram, pattern_capture, " +
                        "pattern_replace, persian_normalization, porter_stem, reverse, " +
                        "russian_stem, shingle, snowball, standard, stemmer, stemmer_override, " +
                        "stop, synonym, trim, truncate, type_as_payload, unique, word_delimiter"));
    }

    @Test
    public void testBuiltInCharFilters() throws Exception {
        List<String> charFilters = new ArrayList<>(analyzerService.getBuiltInCharFilters());
        Collections.sort(charFilters);
        assertThat(Joiner.on(", ").join(charFilters),
                is("htmlStrip, html_strip, mapping, pattern_replace"));
    }

}
