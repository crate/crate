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

import com.amazonaws.Protocol;
import com.google.common.collect.ImmutableMap;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.repositories.url.URLRepository;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;


public class RepositorySettingsModule extends AbstractModule {

    private static final String FS = "fs";
    private static final String URL = "url";
    private static final String HDFS = "hdfs";
    private static final String S3 = "s3";

    private static final TypeSettings FS_SETTINGS = new TypeSettings(
        ImmutableMap.of("location", FsRepository.LOCATION_SETTING),
        ImmutableMap.of(
            "compress", FsRepository.COMPRESS_SETTING,
            "chunk_size", FsRepository.CHUNK_SIZE_SETTING
        ));

    private static final TypeSettings URL_SETTINGS = new TypeSettings(
        ImmutableMap.of("url", URLRepository.URL_SETTING),
        Collections.emptyMap());

    private static final TypeSettings HDFS_SETTINGS = new TypeSettings(
        Collections.emptyMap(),
        ImmutableMap.<String, Setting>builder()
            .put("uri", Setting.simpleString("uri", Setting.Property.NodeScope))
            .put("user", Setting.simpleString("user", Setting.Property.NodeScope))
            .put("path", Setting.simpleString("path", Setting.Property.NodeScope))
            .put("load_defaults", Setting.boolSetting("load_defaults", true, Setting.Property.NodeScope))
            .put("conf_location", Setting.simpleString("conf_location", Setting.Property.NodeScope))
            .put("concurrent_streams", Setting.intSetting("concurrent_streams", 5, Setting.Property.NodeScope))
            .put("compress", Setting.boolSetting("compress", true, Setting.Property.NodeScope))
            // We cannot use a ByteSize setting as it doesn't support NULL and it must be NULL as default to indicate to
            // not override the default behaviour.
            .put("chunk_size", Setting.simpleString("chunk_size"))
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

    /**
     * Default is to use 100MB (S3 defaults) for heaps above 2GB and 5% of
     * the available memory for smaller heaps.
     */
    private static final ByteSizeValue S3_DEFAULT_BUFFER_SIZE = new ByteSizeValue(
        Math.max(
            ByteSizeUnit.MB.toBytes(5), // minimum value
            Math.min(
                ByteSizeUnit.MB.toBytes(100),
                JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)),
        ByteSizeUnit.BYTES);


    private static final TypeSettings S3_SETTINGS = new TypeSettings(
        Collections.emptyMap(),
        ImmutableMap.<String, Setting>builder()
            .put("base_path", Setting.simpleString("base_path"))
            .put("bucket", Setting.simpleString("bucket"))
            .put("client", Setting.simpleString("client"))
            .put("buffer_size", Setting.byteSizeSetting("buffer_size", S3_DEFAULT_BUFFER_SIZE,
                new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(5, ByteSizeUnit.GB)))
            .put("canned_acl", Setting.simpleString("canned_acl"))
            .put("chunk_size", Setting.byteSizeSetting("chunk_size", new ByteSizeValue(1, ByteSizeUnit.GB),
                new ByteSizeValue(5, ByteSizeUnit.MB), new ByteSizeValue(5, ByteSizeUnit.TB)))
            .put("compress", Setting.boolSetting("compress", true)) // TODO: ES defaults to false!
            .put("server_side_encryption",
                Setting.boolSetting("server_side_encryption", false))
            // client related settings
            .put("access_key", SecureSetting.insecureString("access_key"))
            .put("secret_key", SecureSetting.insecureString("secret_key"))
            .put("endpoint", Setting.simpleString("endpoint")
            )
            .put("protocol", new Setting<>("protocol", "https", s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT))))
            .put("max_retries", Setting.intSetting("max_retries", 3))
            .put("use_throttle_retries", Setting.boolSetting("use_throttle_retries", true))
            .build());

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
