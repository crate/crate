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

package io.crate.auth;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.netty.handler.ssl.ClientAuth;

public class AuthSettingsTest {


    @Test
    public void test_cliet_auth_is_none_if_no_hba_entry_has_client_cert_method() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.config.1.method", "trust")
            .put("auth.host_based.config.1.ssl", "on")
            .put("auth.host_based.config.2.method", "password")
            .put("auth.host_based.config.2.ssl", "password")
            .build();

        assertThat(AuthSettings.resolveClientAuth(settings, null), is(ClientAuth.NONE));
    }

    @Test
    public void test_client_auth_is_required_if_all_hba_entries_have_cert_method() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.config.1.method", "cert")
            .put("auth.host_based.config.2.method", "cert")
            .build();

        assertThat(AuthSettings.resolveClientAuth(settings, null), is(ClientAuth.REQUIRE));
    }

    @Test
    public void test_client_auth_is_optional_if_one_hba_entry_is_client_cert_trust() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.config.1.method", "cert")
            .put("auth.host_based.config.2.method", "password")
            .build();
        assertThat(AuthSettings.resolveClientAuth(settings, null), is(ClientAuth.OPTIONAL));
    }

    @Test
    public void test_client_auth_only_counts_entries_matching_protocol_or_no_protocol() throws Exception {
        Settings settings = Settings.builder()
            .put("auth.host_based.config.1.method", "cert")
            .put("auth.host_based.config.1.protocol", "transport")
            .put("auth.host_based.config.2.method", "password")
            .build();
        assertThat(AuthSettings.resolveClientAuth(settings, Protocol.TRANSPORT), is(ClientAuth.REQUIRE));
        assertThat(AuthSettings.resolveClientAuth(settings, Protocol.HTTP), is(ClientAuth.NONE));
        assertThat(AuthSettings.resolveClientAuth(settings, null), is(ClientAuth.OPTIONAL));
    }
}
