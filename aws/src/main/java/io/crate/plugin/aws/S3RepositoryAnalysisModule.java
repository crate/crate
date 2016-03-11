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

package io.crate.plugin.aws;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.SettingsApplier;
import io.crate.analyze.SettingsAppliers;
import io.crate.analyze.repositories.TypeSettings;
import io.crate.metadata.settings.BoolSetting;
import io.crate.metadata.settings.IntSetting;
import io.crate.metadata.settings.StringSetting;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;

import java.util.Collections;

public class S3RepositoryAnalysisModule extends AbstractModule {

    private MapBinder<String, TypeSettings> typeSettingsBinder;

    public static final String REPOSITORY_TYPE = "s3";
    public static final TypeSettings S3_SETTINGS = new TypeSettings(Collections.<String, SettingsApplier>emptyMap(),
            ImmutableMap.<String, SettingsApplier>builder()
                    .put("access_key", new SettingsAppliers.StringSettingsApplier(new StringSetting("access_key", null, true)))
                    .put("base_path", new SettingsAppliers.StringSettingsApplier(new StringSetting("base_path", null, true)))
                    .put("bucket", new SettingsAppliers.StringSettingsApplier(new StringSetting("bucket", null, true)))
                    .put("buffer_size", new SettingsAppliers.IntSettingsApplier(new IntSetting("buffer_size", null, true)))
                    .put("canned_acl", new SettingsAppliers.StringSettingsApplier(new StringSetting("canned_acl", null, true)))
                    .put("chunk_size", new SettingsAppliers.IntSettingsApplier(new IntSetting("chunk_size", null, true)))
                    .put("compress", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("compress", true, true)))
                    .put("concurrent_streams", new SettingsAppliers.IntSettingsApplier(new IntSetting("concurrent_streams", null, true)))
                    .put("endpoint", new SettingsAppliers.StringSettingsApplier(new StringSetting("endpoint", null, true)))
                    .put("max_retries", new SettingsAppliers.IntSettingsApplier(new IntSetting("max_retries", null, true)))
                    .put("protocol", new SettingsAppliers.StringSettingsApplier(new StringSetting("protocol", null, true)))
                    .put("region", new SettingsAppliers.StringSettingsApplier(new StringSetting("region", null, true)))
                    .put("secret_key", new SettingsAppliers.StringSettingsApplier(new StringSetting("secret_key", null, true)))
                    .put("server_side_encryption", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("server_side_encryption", false, true))).build());

    private final Settings settings;

    public S3RepositoryAnalysisModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        typeSettingsBinder = MapBinder.newMapBinder(binder(), String.class, TypeSettings.class);
        if (settings.getAsBoolean("cloud.enabled", true)) {
            typeSettingsBinder.addBinding(REPOSITORY_TYPE).toInstance(S3_SETTINGS);
        }
    }
}
