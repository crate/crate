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

package io.crate.user;

import io.crate.data.Row;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.GenericProperties;
import org.elasticsearch.test.ESTestCase;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.SecureString;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Map;

public class UserActionsTest extends ESTestCase {

    private static final NodeContext nodeCtx = new NodeContext(new Functions(Map.of()));

    TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();

    @Test
    public void testSecureHashIsGeneratedFromPasswordProperty() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("password", Literal.of("password")));
        SecureHash secureHash = UserActions.generateSecureHash(properties, Row.EMPTY, txnCtx, nodeCtx);
        assertThat(secureHash, Matchers.notNullValue());

        SecureString password = new SecureString("password".toCharArray());
        assertTrue(secureHash.verifyHash(password));
    }

    @Test
    public void testNoSecureHashIfPasswordPropertyNotPresent() throws Exception {
        SecureHash secureHash = UserActions.generateSecureHash(GenericProperties.empty(), Row.EMPTY, txnCtx, nodeCtx);
        assertNull(secureHash);
    }

    @Test
    public void testPasswordMustNotBeEmptyErrorIsRaisedIfPasswordIsEmpty() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("password", Literal.of("")));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Password must not be empty");
        UserActions.generateSecureHash(properties, Row.EMPTY, txnCtx, nodeCtx);
    }

    @Test
    public void testUserPasswordProperty() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("password", Literal.of("my-pass")));
        SecureString password = UserActions.getUserPasswordProperty(properties, Row.EMPTY, txnCtx, nodeCtx);
        assertEquals(new SecureString("my-pass".toCharArray()), password);
    }

    @Test
    public void testNoPasswordIfPropertyIsNull() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(
            Map.of("password", Literal.of(DataTypes.UNDEFINED, null)));
        SecureString password = UserActions.getUserPasswordProperty(properties, Row.EMPTY, txnCtx, nodeCtx);
        assertNull(password);
    }

    @Test
    public void testInvalidPasswordProperty() throws Exception {
        GenericProperties<Symbol> properties = new GenericProperties<>(Map.of("invalid", Literal.of("password")));
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"invalid\" is not a valid user property");
        UserActions.generateSecureHash(properties, Row.EMPTY, txnCtx, nodeCtx);
    }
}
