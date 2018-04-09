/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.node.ddl;


import io.crate.expression.symbol.SelectSymbol;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.ddl.tables.CreateAnalyzerTask;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.DependencyCarrier;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class CreateAnalyzerPlan implements Plan {

    private final Settings analyzerSettings;

    public CreateAnalyzerPlan(Settings analyzerSettings) {
        this.analyzerSettings = analyzerSettings;
    }

    public Settings createAnalyzerSettings() {
        return analyzerSettings;
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        Map<SelectSymbol, Object> valuesBySubQuery) {
        CreateAnalyzerTask task = new CreateAnalyzerTask(
            this, executor.transportActionProvider().transportClusterUpdateSettingsAction());
        task.execute(consumer);
    }
}
