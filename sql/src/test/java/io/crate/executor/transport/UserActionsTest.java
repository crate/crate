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

package io.crate.executor.transport;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.data.Row;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.StringLiteral;
import io.crate.test.integration.CrateUnitTest;
import io.crate.user.SecureHash;
import org.elasticsearch.common.settings.SecureString;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class UserActionsTest extends CrateUnitTest {

    @Test
    public void testUserActionExecution() throws Exception {
        AtomicReference<Map<String, SecureHash>> userRef = new AtomicReference<>();

        GenericProperties properties = new GenericProperties();
        properties.add(new GenericProperty("password", new StringLiteral("password")));

        CreateUserAnalyzedStatement statement = new CreateUserAnalyzedStatement("foo", properties);

        UserActions.execute((user, attributes) -> {
            userRef.set(ImmutableMap.of(user, attributes));
            return CompletableFuture.completedFuture(1L);
        }, statement, Row.EMPTY);
        Map<String, SecureHash> actualUser = userRef.get();

        SecureString password = new SecureString("password".toCharArray());
        assertTrue(actualUser.get("foo").verifyHash(password));
        assertTrue(actualUser.containsKey("foo"));
    }

    @Test
    public void testUserActionWithoutUserAttribute() throws Exception {
        AtomicReference<Map<String, SecureHash>> userRef = new AtomicReference<>();

        CreateUserAnalyzedStatement statement = new CreateUserAnalyzedStatement("foo", GenericProperties.EMPTY);

        UserActions.execute((user, secureHash) -> {
            Map<String, SecureHash> userMeta = new HashMap<>();
            userMeta.put(user, secureHash);
            userRef.set(userMeta);
            return CompletableFuture.completedFuture(1L);
        }, statement, Row.EMPTY);
        Map<String, SecureHash> actualUser = userRef.get();
        assertNull(actualUser.get("foo"));
        assertTrue(actualUser.containsKey("foo"));
    }

    @Test
    public void testUserPasswordProperty() throws Exception {
        GenericProperties properties = new GenericProperties();
        properties.add(new GenericProperty("password", new StringLiteral("my-pass")));
        SecureString password = UserActions.getUserPasswordProperty(properties, Row.EMPTY);
        assertEquals(new SecureString("my-pass".toCharArray()), password);
    }

    @Test
    public void testInvalidPasswordProperty() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("\"invalid\" is not a valid setting for CREATE USER");
        GenericProperties properties = new GenericProperties();
        properties.add(new GenericProperty("invalid", new StringLiteral("password")));
        UserActions.getUserPasswordProperty(properties, Row.EMPTY);
    }
}
