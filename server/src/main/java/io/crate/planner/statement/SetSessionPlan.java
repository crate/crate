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

package io.crate.planner.statement;

import static io.crate.data.SentinelRow.SENTINEL;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.Assignment;

public class SetSessionPlan implements Plan {

    private static final Logger LOGGER = LogManager.getLogger(SetSessionPlan.class);

    private final List<Assignment<Symbol>> settings;

    private final SessionSettingRegistry sessionSettingRegistry;

    public SetSessionPlan(List<Assignment<Symbol>> settings, SessionSettingRegistry sessionSettingRegistry) {
        this.settings = settings;
        this.sessionSettingRegistry = sessionSettingRegistry;
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void executeOrFail(DependencyCarrier executor,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {

        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            x,
            params,
            subQueryResults
        );

        var sessionSettings = plannerContext.transactionContext().sessionSettings();
        Assignment<Symbol> assignment = settings.get(0);
        String settingName = eval.apply(assignment.columnName()).toString();
        SessionSetting<?> sessionSetting = sessionSettingRegistry.settings().get(settingName);
        if (sessionSetting == null) {
            LOGGER.info("SET SESSION STATEMENT WILL BE IGNORED: {}", settingName);
            ensureNotGlobalSetting(settingName);
        } else {
            sessionSetting.apply(sessionSettings, assignment.expressions(), eval);
        }
        consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
    }

    @VisibleForTesting
    static void ensureNotGlobalSetting(String settingName) {
        List<String> nameParts = CrateSettings.settingNamesByPrefix(settingName);
        if (nameParts.size() != 0) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "GLOBAL Cluster setting '%s' cannot be used with SET SESSION / LOCAL",
                settingName));
        }
    }
}
