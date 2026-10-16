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

package io.crate.copy.gcs;

import static io.crate.copy.gcs.GCSStorageSettings.validate;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class GCSStorageSettingsTest {

    private static Settings.Builder validCredentials() {
        return Settings.builder()
            .put("project_id", "test")
            .put("private_key_id", "test")
            .put("private_key", "test")
            .put("client_email", "test")
            .put("client_id", "test");
    }

    @Test
    public void test_unknown_setting_is_rejected() {
        Settings settings = validCredentials()
            .put("dummy", "dummy")
            .build();
        assertThatThrownBy(() -> validate(settings, true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'dummy' is not supported");
    }

    @Test
    public void test_auth_param_is_required() {
        assertThatThrownBy(() -> validate(Settings.EMPTY, true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Required credential settings are missing: project_id, private_key_id, private_key, client_email, client_id");
    }

    @Test
    public void test_common_copy_from_settings_accepted() {
        Settings settings = validCredentials()
            .put("fail_fast", "true")
            .build();
        validate(settings, true);
    }

    @Test
    public void test_common_copy_to_settings_accepted() {
        Settings settings = validCredentials()
            .put("compression", "gzip")
            .build();
        validate(settings, false);
    }

    @Test
    public void test_copy_from_only_setting_rejected_for_copy_to() {
        Settings settings = validCredentials()
            .put("fail_fast", "true")
            .build();
        assertThatThrownBy(() -> validate(settings, false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'fail_fast' is not supported");
    }

    @Test
    public void test_partial_credentials_are_rejected() {
        Settings settings = Settings.builder()
            .put("project_id", "test")
            .put("private_key", "test")
            .build();
        assertThatThrownBy(() -> validate(settings, true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("private_key_id")
            .hasMessageContaining("client_email")
            .hasMessageContaining("client_id");
    }
}
