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

package io.crate.plugin;

import io.crate.Plugin;
import io.crate.module.HyperLogLogModule;
import io.crate.settings.SharedSettings;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;

import java.util.Collection;
import java.util.Collections;

public class HyperLogLogPlugin implements Plugin {

    private final boolean isEnabled;

    public HyperLogLogPlugin(Settings settings) {
        isEnabled = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
    }

    @Override
    public String name() {
        return "hyperLogLog";
    }

    @Override
    public String description() {
        return "CrateDB HyperLogLog Plugin";
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return isEnabled ? Collections.singletonList(new HyperLogLogModule()) : Collections.emptyList();
    }
}
