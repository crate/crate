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

import io.crate.analyze.AnalyzedResetStatement;
import io.crate.analyze.SymbolEvaluator;
import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.settings.CrateSettings;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

public final class ResetSettingsPlan implements Plan {

    private final AnalyzedResetStatement resetAnalyzedStatement;

    public ResetSettingsPlan(AnalyzedResetStatement resetAnalyzedStatement) {
        this.resetAnalyzedStatement = resetAnalyzedStatement;
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {

        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(plannerContext.transactionContext(),
                                                                              plannerContext.nodeContext(),
                                                                              x,
                                                                              params,
                                                                              subQueryResults);

        Settings settings = buildSettingsFrom(resetAnalyzedStatement.settingsToRemove(), eval);

        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest()
            .persistentSettings(settings)
            .transientSettings(settings);
        OneRowActionListener<ClusterUpdateSettingsResponse> actionListener = new OneRowActionListener<>(
            consumer,
            r -> r.isAcknowledged() ? new Row1(1L) : new Row1(0L));
        dependencies.client().execute(ClusterUpdateSettingsAction.INSTANCE, request).whenComplete(actionListener);
    }

    @VisibleForTesting
    static Settings buildSettingsFrom(Set<Symbol> settings, Function<? super Symbol, Object> eval) {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Symbol symbol : settings) {
            String settingsName = eval.apply(symbol).toString();
            List<String> settingNames = CrateSettings.settingNamesByPrefix(settingsName);
            if (settingNames.size() == 0) {
                throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                                 "Setting '%s' is not supported",
                                                                 settingsName));
            }
            for (String name : settingNames) {
                CrateSettings.checkIfRuntimeSetting(name);
                if (CrateSettings.isValidSetting(name) == false) {
                    throw new IllegalArgumentException("Setting '" + settingNames + "' is not supported");
                }
                settingsBuilder.put(name, (String) null);
            }
        }
        return settingsBuilder.build();
    }
}

