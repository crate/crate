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

import java.util.Collection;
import java.util.function.Function;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.common.settings.Settings;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Lists;
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
import io.crate.sql.tree.Assignment;

public final class UpdateSettingsPlan implements Plan {

    private final Collection<Assignment<Symbol>> settings;
    private final boolean isPersistent;

    public UpdateSettingsPlan(Collection<Assignment<Symbol>> settings, boolean isPersistent) {
        this.settings = settings;
        this.isPersistent = isPersistent;
    }

    @VisibleForTesting
    public Collection<Assignment<Symbol>> settings() {
        return settings;
    }

    @VisibleForTesting
    public boolean isPersistent() {
        return isPersistent;
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
        ClusterUpdateSettingsRequest request = isPersistent
            ? new ClusterUpdateSettingsRequest().persistentSettings(buildSettingsFrom(settings, eval))
            : new ClusterUpdateSettingsRequest().transientSettings(buildSettingsFrom(settings, eval));

        OneRowActionListener<ClusterUpdateSettingsResponse> actionListener = new OneRowActionListener<>(
            consumer,
            r -> r.isAcknowledged() ? new Row1(1L) : new Row1(0L));
        dependencies.client().execute(ClusterUpdateSettingsAction.INSTANCE, request)
            .whenComplete(actionListener);
    }

    @VisibleForTesting
    static Settings buildSettingsFrom(Collection<Assignment<Symbol>> assignments,
                                      Function<? super Symbol, Object> eval) {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Assignment<Symbol> entry : assignments) {
            String settingsName = eval.apply(entry.columnName()).toString();

            if (CrateSettings.isValidSetting(settingsName) == false) {
                throw new IllegalArgumentException("Setting '" + settingsName + "' is not supported");
            }
            Symbol expression = Lists.getOnlyElement(entry.expressions());
            Object value = eval.apply(expression);
            CrateSettings.flattenSettings(settingsBuilder, settingsName, value);
        }

        Settings settings = settingsBuilder.build();
        for (String checkForRuntime : settings.keySet()) {
            CrateSettings.checkIfRuntimeSetting(checkForRuntime);
        }
        return settings;
    }
}
