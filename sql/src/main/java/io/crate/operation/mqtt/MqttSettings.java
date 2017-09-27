/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.mqtt;

import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

import java.util.function.Function;

public final class MqttSettings {

    private MqttSettings() {
    }

    public static final CrateSetting<Boolean> MQTT_ENABLED_SETTING = CrateSetting.of(
        Setting.boolSetting("ingestion.mqtt.enabled", false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);

    public static final CrateSetting<Boolean> SSL_MQTT_ENABLED = CrateSetting.of(
        Setting.boolSetting("ssl.ingestion.mqtt.enabled", false, Setting.Property.NodeScope),
        DataTypes.BOOLEAN);

    public static final CrateSetting<String> MQTT_PORT_SETTING = CrateSetting.of(new Setting<>(
        "ingestion.mqtt.port", "1883",
        Function.identity(), Setting.Property.NodeScope), DataTypes.STRING);

    public static final CrateSetting<TimeValue> MQTT_TIMEOUT_SETTING = CrateSetting.of(Setting.timeSetting(
        "ingestion.mqtt.timeout", TimeValue.timeValueSeconds(10L), TimeValue.timeValueSeconds(1L),
        Setting.Property.NodeScope), DataTypes.STRING);
}
