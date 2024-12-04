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

import java.util.function.Function;

import io.crate.analyze.AnalyzedCreateRole;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.role.Role;
import io.crate.role.Role.Properties;
import io.crate.role.RoleManager;
import io.crate.sql.tree.GenericProperties;

public class CreateRolePlan implements Plan {

    private final AnalyzedCreateRole createRole;
    private final RoleManager roleManager;
    private final SessionSettingRegistry sessionSettingRegistry;

    public CreateRolePlan(AnalyzedCreateRole createRole,
                          RoleManager roleManager,
                          SessionSettingRegistry sessionSettingRegistry) {
        this.createRole = createRole;
        this.roleManager = roleManager;
        this.sessionSettingRegistry = sessionSettingRegistry;
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
                              SubQueryResults subQueryResults) throws Exception {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            x,
            params,
            subQueryResults
        );
        GenericProperties<Object> evaluatedProperties = createRole.properties().map(eval);
        Properties roleProperties = Role.Properties.of(
            createRole.isUser(),
            false,
            evaluatedProperties,
            sessionSettingRegistry);
        if (roleProperties.login() == false && roleProperties.password() != null) {
            throw new UnsupportedOperationException(
                "Creating a ROLE with a password is not allowed, use CREATE USER instead");
        }
        roleManager.createRole(
                createRole.roleName(),
                createRole.isUser(),
                roleProperties.password(),
                roleProperties.jwtProperties())
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }
}
