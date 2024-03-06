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

package io.crate.analyze;

import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.ANALYZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.CHAR_FILTER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKENIZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKEN_FILTER;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.RowN;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.planner.PlannerContext;
import io.crate.planner.node.ddl.CreateAnalyzerPlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CreateAnalyzerAnalyzerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .setAnalysisPlugins(List.of(new CommonAnalysisPlugin()))
            .build();
        plannerContext = e.getPlannerContext(clusterService.state());
    }

    private ClusterUpdateSettingsRequest analyze(String stmt, Object... arguments) {
        AnalyzedCreateAnalyzer analyzedStatement = e.analyze(stmt);
        return CreateAnalyzerPlan.createRequest(
            analyzedStatement,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            new RowN(arguments),
            SubQueryResults.EMPTY,
            e.fulltextAnalyzerResolver());
    }

    @Test
    public void testCreateAnalyzerSimple() throws Exception {
        ClusterUpdateSettingsRequest request = analyze(
            "CREATE ANALYZER a1 (TOKENIZER lowercase)");
        assertThat(extractAnalyzerSettings("a1", request.persistentSettings()))
            .hasEntry("index.analysis.analyzer.a1.tokenizer", "lowercase")
            .hasEntry("index.analysis.analyzer.a1.type", "custom");
    }

    @Test
    public void testCreateAnalyzerWithCustomTokenizer() throws Exception {
        ClusterUpdateSettingsRequest request = analyze(
            "CREATE ANALYZER a2 (" +
            "   TOKENIZER tok2 with (" +
            "       type='ngram'," +
            "       \"min_ngram\"=2," +
            "       \"token_chars\"=['letter', 'digits']" +
            "   )" +
            ")");
        assertThat(extractAnalyzerSettings("a2", request.persistentSettings()))
            .hasEntry("index.analysis.analyzer.a2.tokenizer", "a2_tok2")
            .hasEntry("index.analysis.analyzer.a2.type", "custom");

        var tokenizerSettings = FulltextAnalyzerResolver.decodeSettings(
            request.persistentSettings().get(TOKENIZER.buildSettingName("a2_tok2")));
        assertThat(tokenizerSettings)
            .hasEntry("index.analysis.tokenizer.a2_tok2.min_ngram", "2")
            .hasEntry("index.analysis.tokenizer.a2_tok2.type", "ngram")
            .hasEntry("index.analysis.tokenizer.a2_tok2.token_chars", "[letter, digits]");
    }

    @Test
    public void testCreateAnalyzerWithCharFilters() throws Exception {
        ClusterUpdateSettingsRequest request = analyze(
            "CREATE ANALYZER a3 (" +
            "   TOKENIZER lowercase," +
            "   CHAR_FILTERS (" +
            "       \"html_strip\"," +
            "       my_mapping WITH (" +
            "           type='mapping'," +
            "           mappings=['ph=>f', 'ß=>ss', 'ö=>oe']" +
            "       )" +
            "   )" +
            ")");

        assertThat(extractAnalyzerSettings("a3", request.persistentSettings()))
            .hasEntry("index.analysis.analyzer.a3.tokenizer", "lowercase")
            .hasEntry("index.analysis.analyzer.a3.char_filter", "[html_strip, a3_my_mapping]");

        var charFiltersSettings = FulltextAnalyzerResolver.decodeSettings(
            request.persistentSettings().get(CHAR_FILTER.buildSettingName("a3_my_mapping")));
        assertThat(charFiltersSettings)
            .hasEntry("index.analysis.char_filter.a3_my_mapping.mappings", "[ph=>f, ß=>ss, ö=>oe]")
            .hasEntry("index.analysis.char_filter.a3_my_mapping.type", "mapping");
    }

    @Test
    public void testCreateAnalyzerWithTokenFilters() throws Exception {
        ClusterUpdateSettingsRequest request = analyze(
            "CREATE ANALYZER a11 (" +
            "  TOKENIZER standard," +
            "  TOKEN_FILTERS (" +
            "    lowercase," +
            "    mystop WITH (" +
            "      type='stop'," +
            "      stopword=['the', 'over']" +
            "    )" +
            "  )" +
            ")");

        assertThat(extractAnalyzerSettings("a11", request.persistentSettings()))
            .hasEntry("index.analysis.analyzer.a11.tokenizer", "standard")
            .hasEntry("index.analysis.analyzer.a11.filter", "[lowercase, a11_mystop]");

        var tokenFiltersSettings = FulltextAnalyzerResolver.decodeSettings(
            request.persistentSettings().get(TOKEN_FILTER.buildSettingName("a11_mystop")));
        assertThat(tokenFiltersSettings)
            .hasEntry("index.analysis.filter.a11_mystop.type", "stop")
            .hasEntry("index.analysis.filter.a11_mystop.stopword", "[the, over]");
    }

    @Test
    public void testCreateAnalyzerExtendingBuiltin() throws Exception {
        ClusterUpdateSettingsRequest request = analyze(
            "CREATE ANALYZER a4 EXTENDS " +
            "german WITH (" +
            "   \"stop_words\"=['der', 'die', 'das']" +
            ")");

        assertThat(extractAnalyzerSettings("a4", request.persistentSettings()))
            .hasEntry("index.analysis.analyzer.a4.stop_words", "[der, die, das]")
            .hasEntry("index.analysis.analyzer.a4.type", "german");
    }

    @Test
    public void createAnalyzerWithoutTokenizer() {
        assertThatThrownBy(
            () -> analyze(
                "CREATE ANALYZER a6 (" +
                "  TOKEN_FILTERS (" +
                "    lowercase" +
                "  )" +
                ")"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Tokenizer missing from non-extended analyzer");
    }

    @Test
    public void overrideDefaultAnalyzer() {
        assertThatThrownBy(
            () -> analyze(
                "CREATE ANALYZER \"default\" (" +
                "  TOKENIZER whitespace" +
                ")"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Overriding the default analyzer is forbidden");
    }

    @Test
    public void overrideBuiltInAnalyzer() {
        assertThatThrownBy(
            () -> analyze("CREATE ANALYZER \"keyword\" (" +
                          "  CHAR_FILTERS (" +
                          "    html_strip" +
                          "  )," +
                          "  TOKENIZER standard" +
                          ")"))

            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot override builtin analyzer 'keyword'");
    }

    @Test
    public void missingParameterInCharFilter() {
        assertThatThrownBy(
            () -> analyze(
                "CREATE ANALYZER my_mapping_analyzer (" +
                "  CHAR_FILTERS (" +
                "    \"mapping\"" +
                "  )," +
                "  TOKENIZER whitespace" +
                ")"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("CHAR_FILTER of type 'mapping' needs additional parameters");
    }

    private static Settings extractAnalyzerSettings(String name, Settings persistentSettings) throws IOException {
        return FulltextAnalyzerResolver
            .decodeSettings(persistentSettings.get(ANALYZER.buildSettingName(name)));
    }
}
