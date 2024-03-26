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

import io.crate.analyze.AnalyzedDropAnalyzer;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.Settings;

import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.ANALYZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.CHAR_FILTER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKENIZER;
import static io.crate.metadata.FulltextAnalyzerResolver.CustomType.TOKEN_FILTER;

public class DropAnalyzerPlan implements Plan {

    private final AnalyzedDropAnalyzer dropAnalyzer;

    public DropAnalyzerPlan(AnalyzedDropAnalyzer dropAnalyzer) {
        this.dropAnalyzer = dropAnalyzer;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        ClusterUpdateSettingsRequest request = createRequest(
            dropAnalyzer.name(),
            dependencies.fulltextAnalyzerResolver());

        dependencies.client().execute(ClusterUpdateSettingsAction.INSTANCE, request)
            .whenComplete(new OneRowActionListener<>(consumer, r -> new Row1(1L)));
    }

    @VisibleForTesting
    public static ClusterUpdateSettingsRequest createRequest(String analyzerName,
                                                             FulltextAnalyzerResolver ftResolver) {
        Settings.Builder builder = Settings.builder();
        builder.putNull(ANALYZER.buildSettingName(analyzerName));

        Settings settings = ftResolver.getCustomAnalyzer(analyzerName);

        String tokenizerName = settings.get(ANALYZER.buildSettingChildName(analyzerName, TOKENIZER.getName()));
        if (tokenizerName != null
            && ftResolver.hasCustomThingy(tokenizerName, FulltextAnalyzerResolver.CustomType.TOKENIZER)) {
            builder.putNull(TOKENIZER.buildSettingName(tokenizerName));
        }

        for (String tokenFilterName : settings
            .getAsList(ANALYZER.buildSettingChildName(analyzerName, TOKEN_FILTER.getName()))) {
            if (ftResolver.hasCustomThingy(tokenFilterName, FulltextAnalyzerResolver.CustomType.TOKEN_FILTER)) {
                builder.putNull(TOKEN_FILTER.buildSettingName(tokenFilterName));
            }
        }

        for (String charFilterName : settings
            .getAsList(ANALYZER.buildSettingChildName(analyzerName, CHAR_FILTER.getName()))) {
            if (ftResolver.hasCustomThingy(charFilterName, FulltextAnalyzerResolver.CustomType.CHAR_FILTER)) {
                builder.putNull(CHAR_FILTER.buildSettingName(charFilterName));
            }
        }

        return new ClusterUpdateSettingsRequest()
            .persistentSettings(builder.build());
    }
}
