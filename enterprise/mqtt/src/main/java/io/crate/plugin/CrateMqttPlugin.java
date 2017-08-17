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

import io.crate.mqtt.netty.Netty4MqttServerTransport;
import io.crate.settings.SharedSettings;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CrateMqttPlugin extends Plugin {

    private final boolean isEnterprise;

    public CrateMqttPlugin(Settings settings) {
        isEnterprise = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
    }

    public String name() {
        return "Crate MQTT Plugin";
    }

    public String description() {
        return "A Plugin that implements the server transport for MQTT";
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return Collections.emptyList();
    }

    @Override
    public Settings additionalSettings() {
        return Settings.EMPTY;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Collections.singletonList(Netty4MqttServerTransport.MQTT_PORT_SETTING.setting());
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return isEnterprise ? Collections.singleton(Netty4MqttServerTransport.class) : Collections.emptyList();
    }
}
