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

package io.crate.copy.azure;

import static io.crate.copy.azure.AzureBlobStorageSettings.PROTOCOL_SETTING;
import static io.crate.copy.azure.AzureBlobStorageSettings.validate;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class AzureBlobStorageSettingsTest {

    @Test
    public void test_unknown_setting_is_rejected() throws Exception {
        Settings settings = Settings.builder().put("dummy", "dummy").build();
        assertThatThrownBy(() -> validate(settings, true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'dummy' is not supported");
    }

    @Test
    public void test_auth_param_is_required() throws Exception {
        assertThatThrownBy(() -> validate(Settings.EMPTY, true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Authentication setting must be provided: either sas_token or key");
    }

    @Test
    public void test_unknown_protocol_is_rejected() throws Exception {
        Settings settings = Settings.builder().put(PROTOCOL_SETTING.getKey(), "pg").build();
        assertThatThrownBy(() -> PROTOCOL_SETTING.get(settings))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid protocol `pg`. Expected HTTP or HTTPS");
    }
}
