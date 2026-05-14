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

package io.crate.protocols.http;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Base64;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpTransportSettings;
import org.junit.Test;

public class QuicTokenSecretsTest {

    private static final byte[] SECRET_16 = new byte[16];

    static {
        Arrays.fill(SECRET_16, (byte) 0xAB);
    }

    @Test
    public void test_resolve_generates_secret_when_unset() {
        byte[] secret = QuicTokenSecrets.resolve(Settings.EMPTY);
        assertThat(secret).hasSize(32);
    }

    @Test
    public void test_resolve_parses_configured_hex_secret() {
        String hex = "ab".repeat(16);
        Settings settings = Settings.builder()
            .put(HttpTransportSettings.SETTING_HTTP_QUIC_TOKEN_SECRET.getKey(), hex)
            .build();
        assertThat(QuicTokenSecrets.resolve(settings)).containsExactly(SECRET_16);
    }

    @Test
    public void test_parse_hex_with_prefix() {
        assertThat(QuicTokenSecrets.parse("hex:" + "ab".repeat(16))).containsExactly(SECRET_16);
    }

    @Test
    public void test_parse_base64() {
        String encoded = Base64.getEncoder().encodeToString(SECRET_16);
        assertThat(QuicTokenSecrets.parse(encoded)).containsExactly(SECRET_16);
        assertThat(QuicTokenSecrets.parse("base64:" + encoded)).containsExactly(SECRET_16);
    }

    @Test
    public void test_parse_rejects_too_short_secret() {
        assertThatThrownBy(() -> QuicTokenSecrets.parse("ab".repeat(15)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least 16 bytes");
    }

    @Test
    public void test_parse_rejects_invalid_value() {
        assertThatThrownBy(() -> QuicTokenSecrets.parse("not-a-valid-secret!!!"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("hex or base64");
    }
}
