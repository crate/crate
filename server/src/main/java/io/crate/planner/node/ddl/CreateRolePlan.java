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

import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import io.crate.analyze.AnalyzedCreateRole;
import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Maps;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.role.JwtProperties;
import io.crate.role.RoleManager;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.role.SecureHash;
import io.crate.role.UserActions;
import io.crate.sql.tree.GenericProperties;

public class CreateRolePlan implements Plan {

    public static final String PASSWORD_PROPERTY_KEY = "password";
    public static final String JWT_PROPERTY_KEY = "jwt";


    private final AnalyzedCreateRole createRole;
    private final RoleManager roleManager;

    public CreateRolePlan(AnalyzedCreateRole createRole, RoleManager roleManager) {
        this.createRole = createRole;
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
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {

        Map<String, Object> properties = parse(
            createRole.properties(),
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            params,
            subQueryResults
        );
        SecureHash newPassword = UserActions.generateSecureHash(properties);

        if (createRole.isUser() == false && newPassword != null) {
            throw new UnsupportedOperationException("Creating a ROLE with a password is not allowed, " +
                                                    "use CREATE USER instead");
        }

        JwtProperties jwtProperties = JwtProperties.fromMap(Maps.getOrDefault(properties, JWT_PROPERTY_KEY, Map.of()));

        roleManager.createRole(createRole.roleName(), createRole.isUser(), newPassword, jwtProperties)
            .whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }

    public static Map<String, Object> parse(GenericProperties<Symbol> genericProperties,
                                               TransactionContext txnCtx,
                                               NodeContext nodeContext,
                                               Row params,
                                               SubQueryResults subQueryResults) {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            txnCtx,
            nodeContext,
            x,
            params,
            subQueryResults
        );
        Map<String, Object> parsedProperties = genericProperties.map(eval).properties();
        for (var property : parsedProperties.keySet()) {
            if (PASSWORD_PROPERTY_KEY.equals(property) == false && JWT_PROPERTY_KEY.equals(property) == false) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "\"%s\" is not a valid user property", property));
            }
        }
        return parsedProperties;
    }
}
