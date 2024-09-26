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

package io.crate.copy;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class ConfigurationTest {

    @Test
    public void test_unknown_setting_is_rejected() throws Exception {
        Configuration<?> configuration = new Configuration<>() {
            @Override
            public List<Setting<String>> supportedSettings() {
                return List.of();
            }

            @Override
            public Map<String, String> fromURIAndSettings(OpenDalURI uri, Settings settings) {
                return Map.of();
            }
        };
        Settings settings = Settings.builder().put("dummy", "dummy").build();
        // COPY FROM
        assertThatThrownBy(() -> configuration.validate(settings, true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'dummy' is not supported");

        // COPY TO
        assertThatThrownBy(() -> configuration.validate(settings, false))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'dummy' is not supported");
    }

}
