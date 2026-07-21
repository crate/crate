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

package io.crate.planner.node.ddl;

import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.ANALYZER;
import static io.crate.testing.Asserts.assertThat;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.AnalyzedCreateAnalyzer;
import io.crate.data.RowN;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class CreateAnalyzerPlanTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private PlannerContext plannerContext;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .setAnalysisPlugins(List.of(new CommonAnalysisPlugin()))
            .build();
        plannerContext = e.getPlannerContext();
    }

    private ClusterUpdateSettingsRequest createRequest(String stmt, Object... arguments) {
        Object plan = e.plan(stmt);
        org.assertj.core.api.Assertions.assertThat(plan).isExactlyInstanceOf(CreateAnalyzerPlan.class);

        AnalyzedCreateAnalyzer analyzedStatement = e.analyze(stmt);
        return CreateAnalyzerPlan.createRequest(
            analyzedStatement,
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            new RowN(arguments),
            SubQueryResults.EMPTY,
            e.fulltextAnalyzerResolver());
    }

    private static Settings extractAnalyzerSettings(String name, Settings persistentSettings) throws IOException {
        return FulltextAnalyzerResolver.decodeSettings(persistentSettings.get(ANALYZER.buildSettingName(name)));
    }

    // See https://github.com/crate/crate/issues/19757
    @Test
    public void test_planner_preserves_token_filter_chain_order() throws Exception {
        ClusterUpdateSettingsRequest request = createRequest(
            "CREATE ANALYZER my_analyzer (" +
            "  TOKENIZER whitespace," +
            "  TOKEN_FILTERS (" +
            "    function_filter WITH (" +
            "      type='word_delimiter'," +
            "      split_on_case_change=true," +
            "      preserve_original=true" +
            "    )," +
            "    lowercase" +
            "  )" +
            ")");

        assertThat(extractAnalyzerSettings("my_analyzer", request.persistentSettings()))
            .hasEntry("index.analysis.analyzer.my_analyzer.filter", "[my_analyzer_function_filter, lowercase]");
    }

    // See https://github.com/crate/crate/issues/19757
    @Test
    public void test_planner_preserves_char_filter_chain_order() throws Exception {
        ClusterUpdateSettingsRequest request = createRequest(
            "CREATE ANALYZER my_analyzer (" +
            "  TOKENIZER whitespace," +
            "  CHAR_FILTERS (" +
            "    my_mapping WITH (" +
            "      type='mapping'," +
            "      mappings=['a=>b']" +
            "    )," +
            "    \"html_strip\"" +
            "  )" +
            ")");

        assertThat(extractAnalyzerSettings("my_analyzer", request.persistentSettings()))
            .hasEntry("index.analysis.analyzer.my_analyzer.char_filter", "[my_analyzer_my_mapping, html_strip]");
    }
}
