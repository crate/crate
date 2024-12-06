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

package org.elasticsearch.repositories.azure;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class AzureStorageSettingsTest extends ESTestCase {

    @Test
    public void test_raise_exception_on_endpoint_uri_without_valid_host() {
        {
            var settings = Settings.builder()
                .put("endpoint", "http://invalid.127.0.0.1")
                .build();
            assertThatThrownBy(() -> AzureStorageSettings.getClientSettings(settings))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid endpoint URI: http://invalid.127.0.0.1");
        }
        {
            var settings = Settings.builder()
                .put("endpoint", "http://127.0.0.1")
                .put("secondary_endpoint", "http://invalid.127.0.0.1")
                .build();
            assertThatThrownBy(() -> AzureStorageSettings.getClientSettings(settings))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid secondary_endpoint URI: http://invalid.127.0.0.1");
        }
    }
}
