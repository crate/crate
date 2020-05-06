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

package io.crate.planner.statement;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.SymbolEvaluator;
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
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static io.crate.data.SentinelRow.SENTINEL;

public class SetSessionPlan implements Plan {

    private static final Logger LOGGER = LogManager.getLogger(SetSessionPlan.class);
    private static final String OPTIMIZER_PREFIX = "optimizer_";

    private final List<Assignment<Symbol>> settings;

    public SetSessionPlan(List<Assignment<Symbol>> settings) {
        this.settings = settings;
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

        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(plannerContext.transactionContext(),
                                                                              plannerContext.functions(),
                                                                              x,
                                                                              params,
                                                                              subQueryResults);

        SessionContext sessionContext = plannerContext.transactionContext().sessionContext();
        Assignment<Symbol> assignment = settings.get(0);
        String settingName = eval.apply(assignment.columnName()).toString();
        validateSetting(settingName);
        if (settingName.startsWith(OPTIMIZER_PREFIX)) {
            Boolean includeRule = DataTypes.BOOLEAN.value(eval.apply(assignment.expression()));
            if (includeRule != null) {
                String ruleName = settingName.replace(OPTIMIZER_PREFIX, "");
                addOptimizerRuleSetting(sessionContext, ruleName, includeRule);
            }
        } else {
            SessionSetting<?> sessionSetting = SessionSettingRegistry.SETTINGS.get(settingName);
            if (sessionSetting == null) {
                LOGGER.info("SET SESSION STATEMENT WILL BE IGNORED: {}", settingName);
            } else {
                sessionSetting.apply(sessionContext, assignment.expressions(), eval);
            }
        }
        consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
    }

    @VisibleForTesting
    static void validateSetting(String settingName) {
        List<String> nameParts = CrateSettings.settingNamesByPrefix(settingName);
        if (nameParts.size() != 0) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                                                             "GLOBAL Cluster setting '%s' cannot be used with SET SESSION / LOCAL",
                                                             settingName));
        }
    }

    @VisibleForTesting
    static void addOptimizerRuleSetting(SessionContext sessionContext, String rule, boolean includeRule) {
        // Convert setting name to rule classname e.g. merge_filters -> MergeFilters
        var ruleName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, rule);
        var rules = sessionContext.excludedOptimizerRules();
        if (includeRule) {
            rules.remove(ruleName);
        } else {
            rules.add(ruleName);
        }
    }
}
