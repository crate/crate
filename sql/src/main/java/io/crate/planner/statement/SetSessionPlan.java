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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.metadata.settings.session.SessionSettingApplier;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.sql.tree.Expression;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import java.util.List;
import java.util.Map;

import static io.crate.data.SentinelRow.SENTINEL;

public class SetSessionPlan implements Plan {

    private static final Logger LOGGER = Loggers.getLogger(SetSessionPlan.class);

    private final Map<String, List<Expression>> settings;
    private final SessionContext sessionContext;

    public SetSessionPlan(Map<String, List<Expression>> settings, SessionContext sessionContext) {
        this.settings = settings;
        this.sessionContext = sessionContext;
    }

    @Override
    public void execute(DependencyCarrier executor,
                        PlannerContext plannerContext,
                        RowConsumer consumer,
                        Row params,
                        Map<SelectSymbol, Object> valuesBySubQuery) {
        for (Map.Entry<String, List<Expression>> entry : settings.entrySet()) {
            SessionSettingApplier applier = SessionSettingRegistry.getApplier(entry.getKey());
            if (applier == null) {
                LOGGER.warn("SET SESSION STATEMENT WILL BE IGNORED: {}", entry.getKey());
            } else {
                applier.apply(params, entry.getValue(), sessionContext);
            }
        }
        consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
    }
}
