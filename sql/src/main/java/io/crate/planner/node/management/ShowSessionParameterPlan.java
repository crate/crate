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

package io.crate.planner.node.management;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.ShowSessionParameterAnalyzedStatement;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.DataType;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.metadata.settings.session.SessionSettingRegistry.HASH_JOIN_KEY;
import static io.crate.metadata.settings.session.SessionSettingRegistry.SEARCH_PATH_KEY;
import static io.crate.metadata.settings.session.SessionSettingRegistry.SEMI_JOIN_KEY;
import static io.crate.metadata.settings.session.SessionSettingRegistry.dataTypeOfParameter;

public class ShowSessionParameterPlan implements Plan {

    private final ShowSessionParameterAnalyzedStatement statement;

    public ShowSessionParameterPlan(ShowSessionParameterAnalyzedStatement statement) {
        this.statement = statement;
    }

    @Override
    public StatementType type() {
        return StatementType.MANAGEMENT;
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        SubQueryResults subQueryResults) {
        String parameterName = statement.parameterName();
        Row1 row;
        try {
            DataType parameterType = dataTypeOfParameter(parameterName);
            Object parameterValue = resolveParameterValue(
                parameterName, plannerContext.transactionContext().sessionContext());
            row = new Row1(parameterType.value(parameterValue));
        } catch (Throwable t) {
            consumer.accept(null, t);
            return;
        }
        consumer.accept(InMemoryBatchIterator.of(row, SENTINEL), null);
    }

    private Object resolveParameterValue(String parameterName, SessionContext sessionContext) {
        switch (parameterName) {
            case SEARCH_PATH_KEY:
                return sessionContext.searchPath();
            case SEMI_JOIN_KEY:
                return sessionContext.getSemiJoinsRewriteEnabled();
            case HASH_JOIN_KEY:
                return sessionContext.isHashJoinEnabled();
            default:
                throw new IllegalArgumentException("Session parameter '" + parameterName + "' not supported");
        }
    }
}
