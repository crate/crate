/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;

public class CreateAnalyzerAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService).enableDefaultTables().build();
    }

    @Test
    public void testCreateAnalyzerSimple() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE ANALYZER a1 (tokenizer lowercase)");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a1", createAnalyzerAnalysis.ident());
        assertEquals("lowercase", createAnalyzerAnalysis.tokenizerDefinition().v1());
        assertEquals(Settings.EMPTY, createAnalyzerAnalysis.tokenizerDefinition().v2());

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void testCreateAnalyzerWithCustomTokenizer() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE ANALYZER a2 (" +
                                                      "   tokenizer tok2 with (" +
                                                      "       type='ngram'," +
                                                      "       \"min_ngram\"=2," +
                                                      "       \"token_chars\"=['letter', 'digits']" +
                                                      "   )" +
                                                      ")");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a2", createAnalyzerAnalysis.ident());
        assertEquals("a2_tok2", createAnalyzerAnalysis.tokenizerDefinition().v1());
        assertThat(
            createAnalyzerAnalysis.tokenizerDefinition().v2().getAsMap(),
            allOf(
                hasEntry("index.analysis.tokenizer.a2_tok2.type", "ngram"),
                hasEntry("index.analysis.tokenizer.a2_tok2.min_ngram", "2"),
                hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.0", "letter"),
                hasEntry("index.analysis.tokenizer.a2_tok2.token_chars.1", "digits")
            )
        );

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void testCreateAnalyzerWithCharFilters() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE ANALYZER a3 (" +
                                                      "   tokenizer lowercase," +
                                                      "   char_filters (" +
                                                      "       \"html_strip\"," +
                                                      "       my_mapping WITH (" +
                                                      "           type='mapping'," +
                                                      "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
                                                      "       )" +
                                                      "   )" +
                                                      ")");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a3", createAnalyzerAnalysis.ident());
        assertEquals("lowercase", createAnalyzerAnalysis.tokenizerDefinition().v1());

        assertThat(
            createAnalyzerAnalysis.charFilters().keySet(),
            containsInAnyOrder("html_strip", "a3_my_mapping")
        );

        assertThat(
            createAnalyzerAnalysis.charFilters().get("a3_my_mapping").getAsMap(),
            hasEntry("index.analysis.char_filter.a3_my_mapping.type", "mapping")
        );
        assertThat(
            createAnalyzerAnalysis.charFilters().get("a3_my_mapping")
                .getAsArray("index.analysis.char_filter.a3_my_mapping.mappings"),
            arrayContainingInAnyOrder("ph=>f", "ß=>ss", "ö=>oe")
        );

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void testCreateAnalyzerWithTokenFilters() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE ANALYZER a11 (" +
                                                      "  TOKENIZER standard," +
                                                      "  TOKEN_FILTERS (" +
                                                      "    lowercase," +
                                                      "    mystop WITH (" +
                                                      "      type='stop'," +
                                                      "      stopword=['the', 'over']" +
                                                      "    )" +
                                                      "  )" +
                                                      ")");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a11", createAnalyzerAnalysis.ident());
        assertEquals("standard", createAnalyzerAnalysis.tokenizerDefinition().v1());

        assertThat(
            createAnalyzerAnalysis.tokenFilters().keySet(),
            containsInAnyOrder("lowercase", "a11_mystop")
        );

        assertThat(
            createAnalyzerAnalysis.tokenFilters().get("a11_mystop").getAsMap(),
            hasEntry("index.analysis.filter.a11_mystop.type", "stop")
        );
        assertThat(
            createAnalyzerAnalysis.tokenFilters().get("a11_mystop")
                .getAsArray("index.analysis.filter.a11_mystop.stopword"),
            arrayContainingInAnyOrder("the", "over")
        );

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void testCreateAnalyzerExtendingBuiltin() throws Exception {
        AnalyzedStatement analyzedStatement = e.analyze("CREATE ANALYZER a4 EXTENDS " +
                                                      "german WITH (" +
                                                      "   \"stop_words\"=['der', 'die', 'das']" +
                                                      ")");
        assertThat(analyzedStatement, instanceOf(CreateAnalyzerAnalyzedStatement.class));
        CreateAnalyzerAnalyzedStatement createAnalyzerAnalysis = (CreateAnalyzerAnalyzedStatement) analyzedStatement;

        assertEquals("a4", createAnalyzerAnalysis.ident());
        assertEquals("german", createAnalyzerAnalysis.extendedAnalyzerName());

        assertThat(
            createAnalyzerAnalysis.genericAnalyzerSettings().getAsArray("index.analysis.analyzer.a4.stop_words"),
            arrayContainingInAnyOrder("der", "die", "das")
        );

        // be sure build succeeds
        createAnalyzerAnalysis.buildSettings();
    }

    @Test
    public void createAnalyzerWithoutTokenizer() throws Exception {
        CreateAnalyzerAnalyzedStatement analysis = e.analyze(
            "CREATE ANALYZER a6 (" +
            "  char_filters (" +
            "    \"html_strip\"" +
            "  )," +
            "  token_filters (" +
            "    lowercase" +
            "  )" +
            ")");
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Tokenizer missing from non-extended analyzer");
        analysis.buildSettings();
    }

    @Test
    public void overrideDefaultAnalyzer() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Overriding the default analyzer is forbidden");
        e.analyze("CREATE ANALYZER \"default\" (" +
                "  TOKENIZER whitespace" +
                ")");
    }

    @Test
    public void overrideBuiltInAnalyzer() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Cannot override builtin analyzer 'keyword'");
        e.analyze("CREATE ANALYZER \"keyword\" (" +
                "  char_filters (" +
                "    html_strip" +
                "  )," +
                "  tokenizer standard" +
                ")");
    }

    @Test
    public void missingParameterInCharFilter() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("CHAR_FILTER of type 'mapping' needs additional parameters");
        CreateAnalyzerAnalyzedStatement analysis = e.analyze(
            "CREATE ANALYZER my_mapping_analyzer (" +
            "  char_filters (" +
            "    \"mapping\"" +
            "  )," +
            "  TOKENIZER whitespace" +
            ")");
        analysis.buildSettings();
    }
}
