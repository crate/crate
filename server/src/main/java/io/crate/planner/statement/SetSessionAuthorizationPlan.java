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

import io.crate.analyze.AnalyzedSetSessionAuthorizationStatement;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.role.Role;
import io.crate.role.RoleLookup;

public class SetSessionAuthorizationPlan implements Plan {

    private final AnalyzedSetSessionAuthorizationStatement setSessionAuthorization;
    private final RoleLookup userLookup;

    public SetSessionAuthorizationPlan(AnalyzedSetSessionAuthorizationStatement setSessionAuthorization,
                                       RoleLookup userLookup) {
        this.setSessionAuthorization = setSessionAuthorization;
        this.userLookup = userLookup;
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
        var sessionSettings = plannerContext.transactionContext().sessionSettings();
        String userName = setSessionAuthorization.user();
        Role user;
        if (userName != null) {
            user = userLookup.findUser(userName);
            if (user == null) {
                throw new IllegalArgumentException("User '" + userName + "' does not exist.");
            }
        } else {
            user = sessionSettings.authenticatedUser();
        }
        sessionSettings.setSessionUser(user);
        consumer.accept(InMemoryBatchIterator.empty(SENTINEL), null);
    }
}
