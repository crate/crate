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

package io.crate.role;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.function.Function;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Maps;
import io.crate.data.Row;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.planner.node.ddl.CreateRolePlan;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.GenericProperties;
import io.crate.types.DataTypes;

public class UserActionsTest extends ESTestCase {

    private static final NodeContext NODE_CTX = new NodeContext(new Functions(Map.of()), null);

    TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    private Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
        txnCtx,
        NODE_CTX,
        x,
        Row.EMPTY,
        SubQueryResults.EMPTY
    );

    @Test
    public void testSecureHashIsGeneratedFromPasswordProperty() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("password", Literal.of("password")));
        SecureHash secureHash = UserActions.generateSecureHash(properties.map(eval).properties());
        assertThat(secureHash).isNotNull();

        SecureString password = new SecureString("password".toCharArray());
        assertThat(secureHash.verifyHash(password)).isTrue();
    }

    @Test
    public void testNoSecureHashIfPasswordPropertyNotPresent() throws Exception {
        GenericProperties<Symbol> properties = GenericProperties.empty();
        SecureHash secureHash = UserActions.generateSecureHash(properties.map(eval).properties());
        assertThat(secureHash).isNull();
    }

    @Test
    public void testPasswordMustNotBeEmptyErrorIsRaisedIfPasswordIsEmpty() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("password", Literal.of("")));

        assertThatThrownBy(() -> UserActions.generateSecureHash(properties.map(eval).properties()))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Password must not be empty");
    }

    @Test
    public void testUserPasswordProperty() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("password", Literal.of("my-pass")));
        SecureString password = UserActions.getUserPasswordProperty(properties.map(eval).properties());
        assertThat(password).isEqualTo(new SecureString("my-pass".toCharArray()));
    }

    @Test
    public void testNoPasswordIfPropertyIsNull() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(
            Map.of("password", Literal.of(DataTypes.UNDEFINED, null)));
        SecureString password = UserActions.getUserPasswordProperty(properties.map(eval).properties());
        assertThat(password).isNull();
    }

    @Test
    public void testInvalidPasswordProperty() throws Exception {
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
            .hasMessage("Only 'iss' and 'username' JWT properties are allowed");
    }
}
