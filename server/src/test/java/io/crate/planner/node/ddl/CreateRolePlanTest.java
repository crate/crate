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

import io.crate.role.Role;
import io.crate.sql.tree.GenericProperties;

public class CreateRolePlanTest {

    @Test
    public void test_invalid_password_property() throws Exception {
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("invalid", "password"));
        assertThatThrownBy(() -> Role.Properties.of(true, properties))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'invalid' is not supported");
    }

    @Test
    public void test_empty_password_string_is_rejected() throws Exception {
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("password", ""));
        assertThatThrownBy(() -> Role.Properties.of(true, properties))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Password must not be empty");
    }

    @Test
    public void test_invalid_jwt_property() throws Exception {
        // Missing jwt property
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("jwt", Map.of("iss", "dummy.org")));
        assertThatThrownBy(() -> Role.Properties.of(true, properties))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("JWT property 'username' must have a non-null value");

        // Invalid jwt property
        GenericProperties<Object> properties2 = new GenericProperties<>(Map.of("jwt",
            Map.of("iss", "dummy.org", "username", "test", "dummy_property", "dummy_val")));
        assertThatThrownBy(() -> Role.Properties.of(true, properties2))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Only 'iss', 'username' and 'aud' JWT properties are allowed");
    }
}
