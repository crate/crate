/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
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
