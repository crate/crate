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

package io.crate.operation.language;

import io.crate.plugin.JavaScriptLanguagePlugin;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

/**
 * A proxy class which implements the elasticsearch Plugin interface
 * so the JavascriptUdfPlugin can be used in integration tests.
 */
public class JavaScriptProxyTestPlugin extends Plugin {

    private final JavaScriptLanguagePlugin plugin;

    public JavaScriptProxyTestPlugin(Settings settings) {
        this.plugin = new JavaScriptLanguagePlugin(settings);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return plugin.createGuiceModules();
    }

    @Override
    public List<Setting<?>> getSettings() {
        return plugin.getSettings();
    }
}
