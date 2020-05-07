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
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.azure.AzureRepository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.repositories.s3.S3Repository;
import org.elasticsearch.repositories.url.URLRepository;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RepositorySettingsModule extends AbstractModule {

    private static final String FS = "fs";
    private static final String URL = "url";
    private static final String HDFS = "hdfs";
    private static final String S3 = "s3";
    private static final String AZURE = "azure";

    private static final TypeSettings FS_SETTINGS = new TypeSettings(
        groupSettingsByKey(FsRepository.mandatorySettings()),
        groupSettingsByKey(FsRepository.optionalSettings())
    );

    private static final TypeSettings URL_SETTINGS = new TypeSettings(
        groupSettingsByKey(URLRepository.mandatorySettings()),
        Map.of()
    );

    private static final TypeSettings HDFS_SETTINGS = new TypeSettings(
        Collections.emptyMap(),
        ImmutableMap.<String, Setting<?>>builder()
            .put("uri", Setting.simpleString("uri", Setting.Property.NodeScope))
            .put("security.principal", Setting.simpleString("security.principal", Setting.Property.NodeScope))
            .put("path", Setting.simpleString("path", Setting.Property.NodeScope))
            .put("load_defaults", Setting.boolSetting("load_defaults", true, Setting.Property.NodeScope))
            .put("compress", Setting.boolSetting("compress", true, Setting.Property.NodeScope))
            // We cannot use a ByteSize setting as it doesn't support NULL and it must be NULL as default to indicate to
            // not override the default behaviour.
            .put("chunk_size", Setting.simpleString("chunk_size"))
            .build()) {

        @Override
        public GenericProperties<?> dynamicProperties(GenericProperties<?> genericProperties) {
            if (genericProperties.isEmpty()) {
                return genericProperties;
            }
            GenericProperties<?> dynamicProperties = new GenericProperties<>();
            for (Map.Entry<String, ?> entry : genericProperties.properties().entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("conf.")) {
                    dynamicProperties.add(new GenericProperty(key, entry.getValue()));
                }
            }
            return dynamicProperties;
        }
    };

    private static final TypeSettings S3_SETTINGS = new TypeSettings(
        groupSettingsByKey(S3Repository.mandatorySettings()),
        groupSettingsByKey(S3Repository.optionalSettings())
    );

    private static final TypeSettings AZURE_SETTINGS = new TypeSettings(
        groupSettingsByKey(AzureRepository.mandatorySettings()),
        groupSettingsByKey(AzureRepository.optionalSettings())
    );

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
        typeSettingsBinder.addBinding(AZURE).toInstance(AZURE_SETTINGS);
    }

    private static Map<String, Setting<?>> groupSettingsByKey(List<Setting<?>> settings) {
        return settings.stream().collect(Collectors.toMap(Setting::getKey, Function.identity()));
    }
}
