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

package io.crate.role;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.junit.Test;

import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.sql.tree.GenericProperties;

public class RolePropertiesTest {

    private final SessionSettingRegistry sessionSettingRegistry = new SessionSettingRegistry(Set.of());

    @Test
    public void test_invalid_property() throws Exception {
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("invalid", "password"));
        assertThatThrownBy(() -> Role.Properties.of(true, false, properties, sessionSettingRegistry))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'invalid' is not supported");
    }

    @Test
    public void test_empty_password_string_is_rejected() throws Exception {
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("password", ""));
        assertThatThrownBy(() -> Role.Properties.of(true, false, properties, sessionSettingRegistry))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Password must not be empty");
    }

    @Test
    public void test_invalid_jwt_property() throws Exception {
        // Missing jwt property
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("jwt", Map.of("iss", "dummy.org")));
        assertThatThrownBy(() -> Role.Properties.of(true, false, properties, sessionSettingRegistry))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("JWT property 'username' must have a non-null value");

        // Invalid jwt property
        GenericProperties<Object> properties2 = new GenericProperties<>(Map.of("jwt",
            Map.of("iss", "dummy.org", "username", "test", "dummy_property", "dummy_val")));
        assertThatThrownBy(() -> Role.Properties.of(true, false, properties2, sessionSettingRegistry))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Only 'iss', 'username' and 'aud' JWT properties are allowed");
    }

    @Test
    public void test_session_property_with_invalid_value() {
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("statement_timeout", "invalid"));
        assertThatThrownBy(() -> Role.Properties.of(true, false, properties, sessionSettingRegistry))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid interval format: invalid");
    }

    @Test
    public void test_session_property_which_is_read_only() {
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("server_version", "1.0.0."));
        assertThatThrownBy(() -> Role.Properties.of(true, false, properties, sessionSettingRegistry))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("\"server_version\" cannot be changed.");
    }

    @Test
    public void test_session_setting_reset_invalid_session_setting() {
        GenericProperties<Object> properties = new GenericProperties<>(Map.of("invalid_property", "NULL"));
        assertThatThrownBy(() -> Role.Properties.of(true, false, properties, sessionSettingRegistry))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'invalid_property' is not supported");
    }

    @Test
    public void test_bwc_streaming() throws Exception {
        var properties = new Role.Properties(
            true,
            SecureHash.of(new SecureString("foo".toCharArray())),
            new JwtProperties("iss", "username", "aud"),
            Map.of("key", "value")
        );

        BytesStreamOutput out = new BytesStreamOutput();
        properties.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_5_6_0);

        Role.Properties actual = new Role.Properties(in);
        assertThat(actual.login()).isEqualTo(properties.login());
        assertThat(actual.password()).isEqualTo(properties.password());
        assertThat(actual.jwtProperties()).isNull();
        assertThat(actual.sessionSettings()).isEqualTo(Map.of());
    }
}
