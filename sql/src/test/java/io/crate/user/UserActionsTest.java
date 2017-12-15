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

package io.crate.user;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.Literal;
import io.crate.data.Row;
import io.crate.metadata.Functions;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.SecureString;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

public class UserActionsTest extends CrateUnitTest {

    private static final Functions functions = new Functions(
        ImmutableMap.of(),
        ImmutableMap.of()
    );

    @Test
    public void testSecureHashIsGeneratedFromPasswordProperty() throws Exception {
        SecureHash secureHash = UserActions.generateSecureHash(
            Collections.singletonMap("password", Literal.of("password")), Row.EMPTY, functions);
        assertThat(secureHash, Matchers.notNullValue());

        SecureString password = new SecureString("password".toCharArray());
        assertTrue(secureHash.verifyHash(password));
    }

    @Test
    public void testNoSecureHashIfPasswordPropertyNotPresent() throws Exception {
        SecureHash secureHash = UserActions.generateSecureHash(emptyMap(), Row.EMPTY, functions);
        assertNull(secureHash);
    }

    @Test
    public void testPasswordMustNotBeEmptyErrorIsRaisedIfPasswordIsEmpty() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Password must not be empty");
        UserActions.generateSecureHash(singletonMap("password", Literal.of("")), Row.EMPTY, functions);
    }

    @Test
    public void testUserPasswordProperty() throws Exception {
        SecureString password = UserActions.getUserPasswordProperty(ImmutableMap.of("password", Literal.of("my-pass")), Row.EMPTY, functions);
        assertEquals(new SecureString("my-pass".toCharArray()), password);
    }

    @Test
    public void testNoPasswordIfPropertyIsNull() throws Exception {
        SecureString password = UserActions.getUserPasswordProperty(ImmutableMap.of("password", Literal.of(DataTypes.UNDEFINED, null)), Row.EMPTY, functions);
        assertNull(password);
    }

    @Test
    public void testInvalidPasswordProperty() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"invalid\" is not a valid user property");
        UserActions.getUserPasswordProperty(ImmutableMap.of("invalid", Literal.of("password")), Row.EMPTY, functions);
    }
}
