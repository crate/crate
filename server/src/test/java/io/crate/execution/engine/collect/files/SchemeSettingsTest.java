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

package io.crate.execution.engine.collect.files;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class SchemeSettingsTest {

    @Test
    public void test_unknown_setting_is_rejected() throws Exception {
        Settings settings = Settings.builder().put("dummy", "dummy").build();
        SchemeSettings schemeSettings = new SchemeSettings(List.of(), List.of());
        assertThatThrownBy(() -> schemeSettings.validate(settings, true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'dummy' is not supported");
    }

    @Test
    public void test_mandatory_setting_not_provided_throws_an_error() throws Exception {
        var mandatory = Setting.simpleString("mandatory");
        var optional = Setting.simpleString("optional");
        Settings settings = Settings.builder().put(optional.getKey(), "dummy").build();
        SchemeSettings schemeSettings = new SchemeSettings(List.of(mandatory), List.of(optional));
        assertThatThrownBy(() -> schemeSettings.validate(settings, true))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Setting 'mandatory' must be provided");
    }
}
