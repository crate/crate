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

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.settings.BoolSetting;
import io.crate.metadata.settings.ByteSizeSetting;
import io.crate.metadata.settings.IntSetting;
import io.crate.metadata.settings.SettingsApplier;
import io.crate.metadata.settings.SettingsAppliers;
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

    private static final TypeSettings FS_SETTINGS = new TypeSettings(
        ImmutableMap.<String, SettingsApplier>of("location", new SettingsAppliers.StringSettingsApplier(new StringSetting("location"))),
        ImmutableMap.<String, SettingsApplier>of(
            "compress", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("compress", true)),
            "chunk_size", new SettingsAppliers.ByteSizeSettingsApplier(new ByteSizeSetting("chunk_size", null))
        ));

    private static final TypeSettings URL_SETTINGS = new TypeSettings(
        ImmutableMap.<String, SettingsApplier>of("url", new SettingsAppliers.StringSettingsApplier(new StringSetting("url"))),
        Collections.<String, SettingsApplier>emptyMap());

    private static final TypeSettings HDFS_SETTINGS = new TypeSettings(
        Collections.<String, SettingsApplier>emptyMap(),
        ImmutableMap.<String, SettingsApplier>builder()
            .put("uri", new SettingsAppliers.StringSettingsApplier(new StringSetting("uri")))
            .put("user", new SettingsAppliers.StringSettingsApplier(new StringSetting("user")))
            .put("path", new SettingsAppliers.StringSettingsApplier(new StringSetting("path")))
            .put("load_defaults", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("load_defaults", true)))
            .put("conf_location", new SettingsAppliers.StringSettingsApplier(new StringSetting("conf_location")))
            .put("concurrent_streams", new SettingsAppliers.IntSettingsApplier(new IntSetting("concurrent_streams", 5)))
            .put("compress", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("compress", true)))
            .put("chunk_size", new SettingsAppliers.ByteSizeSettingsApplier(new ByteSizeSetting("chunk_size", null)))
            .build()) {

        @Override
        public GenericProperties dynamicProperties(GenericProperties genericProperties) {
            if (genericProperties.isEmpty()) {
                return genericProperties;
            }
            GenericProperties dynamicProperties = new GenericProperties();
            for (Map.Entry<String, Expression> entry : genericProperties.properties().entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("conf.")) {
                    dynamicProperties.add(new GenericProperty(key, entry.getValue()));
                }
            }
            return dynamicProperties;
        }
    };

    private static final TypeSettings S3_SETTINGS = new TypeSettings(Collections.<String, SettingsApplier>emptyMap(),
        ImmutableMap.<String, SettingsApplier>builder()
            .put("access_key", new SettingsAppliers.StringSettingsApplier(new StringSetting("access_key", null)))
            .put("base_path", new SettingsAppliers.StringSettingsApplier(new StringSetting("base_path", null)))
            .put("bucket", new SettingsAppliers.StringSettingsApplier(new StringSetting("bucket", null)))
            .put("buffer_size", new SettingsAppliers.ByteSizeSettingsApplier(new ByteSizeSetting("buffer_size", null)))
            .put("canned_acl", new SettingsAppliers.StringSettingsApplier(new StringSetting("canned_acl", null)))
            .put("chunk_size", new SettingsAppliers.ByteSizeSettingsApplier(new ByteSizeSetting("chunk_size", null)))
            .put("compress", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("compress", true)))
            .put("concurrent_streams", new SettingsAppliers.IntSettingsApplier(new IntSetting("concurrent_streams", null)))
            .put("endpoint", new SettingsAppliers.StringSettingsApplier(new StringSetting("endpoint", null)))
            .put("max_retries", new SettingsAppliers.IntSettingsApplier(new IntSetting("max_retries", null)))
            .put("protocol", new SettingsAppliers.StringSettingsApplier(new StringSetting("protocol", null)))
            .put("region", new SettingsAppliers.StringSettingsApplier(new StringSetting("region", null)))
            .put("secret_key", new SettingsAppliers.StringSettingsApplier(new StringSetting("secret_key", null)))
            .put("server_side_encryption", new SettingsAppliers.BooleanSettingsApplier(new BoolSetting("server_side_encryption", false))).build());

    @Override
    protected void configure() {
        MapBinder<String, TypeSettings> typeSettingsBinder = MapBinder.newMapBinder(
            binder(),
            String.class,
            TypeSettings.class);
        typeSettingsBinder.addBinding(FS).toInstance(FS_SETTINGS);
        typeSettingsBinder.addBinding(URL).toInstance(URL_SETTINGS);
        typeSettingsBinder.addBinding(HDFS).toInstance(HDFS_SETTINGS);
        typeSettingsBinder.addBinding(S3).toInstance(S3_SETTINGS);
    }
}
