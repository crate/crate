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

import io.crate.analyze.ShowSessionParameterAnalyzedStatement;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.data.RowN;
import io.crate.metadata.settings.SessionSettings;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;

import java.util.ArrayList;
import java.util.Map;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.metadata.settings.session.SessionSettingRegistry.SETTINGS;

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
    public void executeOrFail(DependencyCarrier executor,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {
        SessionSettings sessionSettings = plannerContext.transactionContext().sessionSettings();
        BatchIterator<Row> batchIterator;
        if (statement.showAll()) {
            batchIterator = iteratorForAllParameters(sessionSettings);
        } else {
            batchIterator = iteratorForSingleParameter(statement.parameterName(), sessionSettings);
        }
        consumer.accept(batchIterator, null);
    }

    private BatchIterator<Row> iteratorForSingleParameter(String parameterName, SessionSettings sessionSettings) {
        SessionSetting sessionSetting = SETTINGS.get(parameterName);
        String parameterValue = sessionSetting.getValue(sessionSettings);
        return InMemoryBatchIterator.of(new Row1(parameterValue), SENTINEL);
    }

    private BatchIterator<Row> iteratorForAllParameters(SessionSettings sessionSettings) {
        ArrayList<Row> rows = new ArrayList<>(SETTINGS.size());
        for (Map.Entry<String, SessionSetting<?>> entry : SETTINGS.entrySet()) {
            Object[] values = new Object[2];
            values[0] = entry.getKey();
            values[1] = entry.getValue().getValue(sessionSettings);
            rows.add(new RowN(values));
        }
        return InMemoryBatchIterator.of(rows, SENTINEL);
    }
}
