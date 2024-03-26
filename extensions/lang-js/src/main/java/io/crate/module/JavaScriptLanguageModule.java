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

package io.crate.module;

import java.util.List;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.operation.language.JavaScriptLanguage;

public class JavaScriptLanguageModule extends AbstractModule {

    public static final Setting<Boolean> LANG_JS_ENABLED =
        Setting.boolSetting("lang.js.enabled", true, Setting.Property.NodeScope);

    private final Settings settings;

    public JavaScriptLanguageModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        if (LANG_JS_ENABLED.get(settings)) {
            bind(JavaScriptLanguage.class).asEagerSingleton();
        }
    }

    public List<Setting<?>> settings() {
        return List.of(LANG_JS_ENABLED);
    }
}
