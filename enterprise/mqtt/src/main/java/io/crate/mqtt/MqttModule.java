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

package io.crate.mqtt;

import com.google.common.collect.ImmutableList;
import io.crate.ingestion.IngestionModules;
import io.crate.mqtt.netty.Netty4MqttServerTransport;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Setting;

import java.util.Collection;
import java.util.Collections;

import static io.crate.mqtt.netty.Netty4MqttServerTransport.MQTT_ENABLED_SETTING;
import static io.crate.mqtt.netty.Netty4MqttServerTransport.MQTT_PORT_SETTING;
import static io.crate.mqtt.netty.Netty4MqttServerTransport.MQTT_TIMEOUT_SETTING;

public class MqttModule extends AbstractModule implements IngestionModules {

    @Override
    protected void configure() {
        bind(Netty4MqttServerTransport.class).asEagerSingleton();
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getServiceClasses() {
        return ImmutableList.of(Netty4MqttServerTransport.class);
    }

    @Override
    public Collection<Module> getModules() {
        return Collections.singletonList(this);
    }

    @Override
    public Collection<Setting<?>> getSettings() {
        return ImmutableList.of(MQTT_ENABLED_SETTING.setting(),
            MQTT_PORT_SETTING.setting(),
            MQTT_TIMEOUT_SETTING.setting());
    }

}
