/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.Test;

import io.crate.common.collections.Maps;
import io.crate.data.Row;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.role.JwtProperties;
import io.crate.sql.tree.GenericProperties;

public class CreateRolePlanTest {

    private static final NodeContext NODE_CTX = new NodeContext(new Functions(Map.of()), null);

    TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Test
    public void test_invalid_password_property() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("invalid", Literal.of("password")));
        assertThatThrownBy(() -> CreateRolePlan.parse(properties, txnCtx, NODE_CTX, Row.EMPTY, SubQueryResults.EMPTY))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("\"invalid\" is not a valid user property");
    }

    @Test
    public void test_invalid_jwt_property() throws Exception {
        // Missing jwt property
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("jwt", Literal.of(Map.of("iss", "dummy.org"))));
        Map<String, Object> parsedProperties = CreateRolePlan.parse(properties, txnCtx, NODE_CTX, Row.EMPTY, SubQueryResults.EMPTY);
        final Map<String, Object> jwtProperties = Maps.getOrDefault(parsedProperties, "jwt", Map.of());
        assertThatThrownBy(() -> JwtProperties.fromMap(jwtProperties))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("JWT property 'username' must have a non-null value");

        // Invalid jwt property
        properties = new GenericProperties<>(Map.of("jwt",
            Literal.of(Map.of("iss", "dummy.org", "username", "test", "dummy_property", "dummy_val"))));
        parsedProperties = CreateRolePlan.parse(properties, txnCtx, NODE_CTX, Row.EMPTY, SubQueryResults.EMPTY);
        final Map<String, Object> invalidJwtProperties = Maps.getOrDefault(parsedProperties, "jwt", Map.of());
        assertThatThrownBy(() -> JwtProperties.fromMap(invalidJwtProperties))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Only 'iss', 'username' and 'aud' JWT properties are allowed");
    }

}
