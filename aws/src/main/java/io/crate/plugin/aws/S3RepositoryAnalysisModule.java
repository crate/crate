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

import com.google.common.collect.ImmutableSet;
import io.crate.analyze.repositories.TypeSettings;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.common.settings.Settings;

public class S3RepositoryAnalysisModule extends AbstractModule {

    private MapBinder<String, TypeSettings> typeSettingsBinder;

    public static final String REPOSITORY_TYPE = "s3";
    public static final TypeSettings S3_SETTINGS = new TypeSettings(ImmutableSet.<String>of(),
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
