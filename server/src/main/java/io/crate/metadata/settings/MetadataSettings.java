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

package io.crate.metadata.settings;

import io.crate.action.sql.SessionContext;
import io.crate.metadata.settings.session.SessionSetting;
import io.crate.metadata.settings.session.SessionSettingProvider;
import io.crate.settings.CrateSetting;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

@Singleton
public final class MetadataSettings implements SessionSettingProvider {

    public static final String EXPOSE_OBJECT_COLUMNS = "expose_object_columns";

    public static final CrateSetting<Boolean> EXPOSE_OBJECT_COLUMNS_SETTING = CrateSetting.of(
        Setting.boolSetting(
            EXPOSE_OBJECT_COLUMNS,
            true,
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        ),
        DataTypes.BOOLEAN
    );

    private volatile boolean exposeObjectColumns = true;

    @Inject
    public MetadataSettings(Settings settings, ClusterSettings clusterSettings) {
        exposeObjectColumns = EXPOSE_OBJECT_COLUMNS_SETTING.setting().get(settings);
        clusterSettings.addSettingsUpdateConsumer(EXPOSE_OBJECT_COLUMNS_SETTING.setting(), newValue -> {
            exposeObjectColumns = newValue;
        });
    }

    public boolean exposeObjectColumns() {
        return exposeObjectColumns;
    }

    @Override
    public List<SessionSetting<?>> sessionSettings() {
        return List.of(
            new SessionSetting<>(
                EXPOSE_OBJECT_COLUMNS,
                objects -> {
                    if (objects.length != 1) {
                        throw new IllegalArgumentException(
                            EXPOSE_OBJECT_COLUMNS + " should have only one argument.");
                    }
                },
                objects -> DataTypes.BOOLEAN.implicitCast(objects[0]),
                SessionContext::setExposeObjectColumns,
                s -> Boolean.toString(s.exposeObjectColumns()),
                () -> String.valueOf(exposeObjectColumns),
                "Whether CrateDB should expose the schema of object data types at system tables.",
                DataTypes.BOOLEAN
            )
        );
    }
}
