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

package org.elasticsearch.common.settings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.elasticsearch.common.settings.Setting.Property;
import org.junit.Test;

import io.crate.server.inject.ModuleTestCase;

public class SettingsModuleTests extends ModuleTestCase {

    @Test
    public void testValidate() {
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "2.0").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]").build();
            assertThatThrownBy(() ->  new SettingsModule(settings))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]");
        }

        {
            Settings settings = Settings.builder().put("cluster.routing.allocation.balance.shard", "[2.0]")
                .put("some.foo.bar", 1).build();
            assertThatThrownBy(() -> new SettingsModule(settings))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Failed to parse value [[2.0]] for setting [cluster.routing.allocation.balance.shard]")
                .satisfies(ex -> {
                    assertThat(ex.getSuppressed()).hasSize(1);
                    assertThat(ex.getSuppressed()[0].getMessage()).isEqualTo(
                        "unknown setting [some.foo.bar] please check that any required plugins are installed, or check the breaking " +
                        "changes documentation for removed settings"
                    );
                });
        }

        {
            Settings settings = Settings.builder().put("index.codec", "default")
                .put("index.foo.bar", 1).build();
            assertThatThrownBy(() -> new SettingsModule(settings))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Index settings found. These have been unsupported since CrateDB 2.0 and should've been migrated.");
        }

        {
            Settings settings = Settings.builder().put("index.codec", "default").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
    }

    @Test
    public void testRegisterSettings() {
        {
            Settings settings = Settings.builder().put("some.custom.setting", "2.0").build();
            SettingsModule module = new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }
        {
            Settings settings = Settings.builder().put("some.custom.setting", "false").build();
            try {
                new SettingsModule(settings, Setting.floatSetting("some.custom.setting", 1.0f, Property.NodeScope));
                fail();
            } catch (IllegalArgumentException ex) {
                assertEquals("Failed to parse value [false] for setting [some.custom.setting]", ex.getMessage());
            }
        }
    }

    @Test
    public void testLoggerSettings() {
        {
            Settings settings = Settings.builder().put("logger._root", "TRACE").put("logger.transport", "INFO").build();
            SettingsModule module = new SettingsModule(settings);
            assertInstanceBinding(module, Settings.class, (s) -> s == settings);
        }

        {
            Settings settings = Settings.builder().put("logger._root", "BOOM").put("logger.transport", "WOW").build();
            assertThatThrownBy(() -> new SettingsModule(settings))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown level constant [BOOM].");
        }
    }

    @Test
    public void testMutuallyExclusiveScopes() {
        new SettingsModule(Settings.EMPTY, Setting.simpleString("foo.bar", Property.NodeScope));
        new SettingsModule(Settings.EMPTY, Setting.simpleString("index.foo.bar", Property.IndexScope));

        // Those should fail
        try {
            new SettingsModule(Settings.EMPTY, Setting.simpleString("foo.bar"));
            fail("No scope should fail");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("No scope found for setting"));
        }
        // Some settings have both scopes - that's fine too if they have per-node defaults
        try {
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope),
                Setting.simpleString("foo.bar", Property.NodeScope));
            fail("already registered");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot register setting [foo.bar] twice"));
        }

        try {
            new SettingsModule(Settings.EMPTY,
                Setting.simpleString("foo.bar", Property.IndexScope, Property.NodeScope),
                Setting.simpleString("foo.bar", Property.IndexScope));
            fail("already registered");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("Cannot register setting [foo.bar] twice"));
        }
    }
}
