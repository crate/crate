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

package io.crate.analyze.repositories;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.SettingsApplier;
import io.crate.analyze.SettingsAppliers;
import io.crate.metadata.settings.BoolSetting;
import io.crate.metadata.settings.ByteSizeSetting;
import io.crate.metadata.settings.IntSetting;
import io.crate.metadata.settings.StringSetting;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.Collections;
import java.util.Map;

public class RepositorySettingsModule extends AbstractModule {

    private static final String FS = "fs";
    private static final String URL = "url";
    private static final String HDFS = "hdfs";
    private static final String S3 = "s3";

    static final TypeSettings FS_SETTINGS = new TypeSettings(
            ImmutableMap.<String, SettingsApplier>of("location", new SettingsAppliers.StringSettingsApplier(new StringSetting("location", true))),
            ImmutableMap.<String, SettingsApplier>of(
                    "compress", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("compress", true, true)),
                    "chunk_size", new SettingsAppliers.ByteSizeSettingsApplier(new ByteSizeSetting("chunk_size", null, true))
            ));

    static final TypeSettings URL_SETTINGS = new TypeSettings(
            ImmutableMap.<String, SettingsApplier>of("url", new SettingsAppliers.StringSettingsApplier(new StringSetting("url", true))),
            Collections.<String, SettingsApplier>emptyMap());


    static final TypeSettings HDFS_SETTINGS = new TypeSettings(
            Collections.<String, SettingsApplier>emptyMap(),
            ImmutableMap.<String, SettingsApplier>builder()
                    .put("uri", new SettingsAppliers.StringSettingsApplier(new StringSetting("uri", true)))
                    .put("user", new SettingsAppliers.StringSettingsApplier(new StringSetting("user", true)))
                    .put("path", new SettingsAppliers.StringSettingsApplier(new StringSetting("path", true)))
                    .put("load_defaults", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("load_defaults", true, true)))
                    .put("conf_location", new SettingsAppliers.StringSettingsApplier(new StringSetting("conf_location", true)))
                    .put("concurrent_streams", new SettingsAppliers.IntSettingsApplier(new IntSetting("concurrent_streams", 5, true)))
                    .put("compress", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("compress", true, true)))
                    .put("chunk_size", new SettingsAppliers.ByteSizeSettingsApplier(new ByteSizeSetting("chunk_size", null, true)))
            .build()) {

        @Override
        public Optional<GenericProperties> dynamicProperties(Optional<GenericProperties> genericProperties) {
            if (!genericProperties.isPresent()) {
                return genericProperties;
            }
            GenericProperties dynamicProperties = null;
            Map<String, Expression> properties = genericProperties.get().properties();
            for(Map.Entry<String, Expression> entry : properties.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("conf.")) {
                    if (dynamicProperties == null) {
                        dynamicProperties = new GenericProperties();
                    }
                    dynamicProperties.add(new GenericProperty(key, entry.getValue()));
                }
            }
            return Optional.fromNullable(dynamicProperties);
        }
    };

     static final TypeSettings S3_SETTINGS = new TypeSettings(Collections.<String, SettingsApplier>emptyMap(),
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

    private MapBinder<String, TypeSettings> typeSettingsBinder;

    @Override
    protected void configure() {
        typeSettingsBinder = MapBinder.newMapBinder(binder(), String.class, TypeSettings.class);
        typeSettingsBinder.addBinding(FS).toInstance(FS_SETTINGS);
        typeSettingsBinder.addBinding(URL).toInstance(URL_SETTINGS);
        typeSettingsBinder.addBinding(HDFS).toInstance(HDFS_SETTINGS);
        typeSettingsBinder.addBinding(S3).toInstance(S3_SETTINGS);
    }
}
