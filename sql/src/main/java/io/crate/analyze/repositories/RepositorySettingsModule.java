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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

public class RepositorySettingsModule extends AbstractModule {

    private static final String FS = "fs";
    private static final String URL = "url";
    private static final String HDFS = "hdfs";
    private static final String S3 = "s3";

    static final TypeSettings FS_SETTINGS = new TypeSettings(ImmutableSet.of("location"),  ImmutableSet.of("compress", "chunk_size"));
    static final TypeSettings URL_SETTINGS = new TypeSettings(ImmutableSet.of("url"), ImmutableSet.<String>of());
    static final TypeSettings HDFS_SETTINGS = new TypeSettings(
            ImmutableSet.<String>of(),
            ImmutableSet.of("uri", "user", "path", "load_defaults", "conf_location", "concurrent_streams", "compress", "chunk_size"));

    static final TypeSettings S3_SETTINGS = new TypeSettings(ImmutableSet.<String>of(),
            ImmutableSet.of(
                    "access_key",
                    "base_path",
                    "bucket",
                    "buffer_size",
                    "canned_acl",
                    "chunk_size",
                    "compress",
                    "concurrent_streams",
                    "endpoint",
                    "max_retries",
                    "protocol",
                    "region",
                    "secret_key",
                    "server_side_encryption"
            ));


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
