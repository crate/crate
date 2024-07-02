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

import io.crate.analyze.AnalyzedAlterRole;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.role.JwtProperties;
import io.crate.role.Role;
import io.crate.role.Role.Properties;
import io.crate.role.RoleManager;
import io.crate.role.SecureHash;
import io.crate.sql.tree.GenericProperties;

public class AlterRolePlan implements Plan {

    private final RoleManager roleManager;
    private final AnalyzedAlterRole alterRole;

    public AlterRolePlan(AnalyzedAlterRole alterRole, RoleManager roleManager) {
        this.alterRole = alterRole;
        this.roleManager = roleManager;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params, SubQueryResults subQueryResults) throws Exception {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            x,
            params,
            subQueryResults
        );
        GenericProperties<Object> evaluatedProperties = alterRole.properties().map(eval);
        Properties roleProperties = Role.Properties.of(false, evaluatedProperties);
        SecureHash newPassword = roleProperties.password();
        boolean resetPassword = evaluatedProperties.contains(Properties.PASSWORD_KEY) && newPassword == null;
        JwtProperties newJwtProperties = roleProperties.jwtProperties();
        boolean resetJwtProperties = evaluatedProperties.contains(Properties.JWT_KEY) && newJwtProperties == null;

        roleManager.alterRole(alterRole.roleName(), newPassword, newJwtProperties, resetPassword, resetJwtProperties)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }
}
