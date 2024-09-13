/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.copy.azure;

import static io.crate.copy.azure.AzureBlobStorageSettings.AZURE_TO_OPEN_DAL;
import static io.crate.copy.azure.AzureBlobStorageSettings.REQUIRED_SETTINGS;
import static io.crate.copy.azure.AzureBlobStorageSettings.SUPPORTED_SETTINGS;
import static io.crate.copy.azure.AzureBlobStorageSettings.validate;
import static io.crate.copy.azure.AzureCopyPlugin.NAME;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.zip.GZIPOutputStream;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.export.FileOutput;

public class AzureFileOutput implements FileOutput {

    private final Map<String, String> config;
    private final Operator operator;

    public AzureFileOutput(SharedAsyncExecutor sharedAsyncExecutor, Settings settings) {
        validate(settings, false);

        config = new HashMap<>();
        for (Setting<String> setting : SUPPORTED_SETTINGS) {
            var value = setting.get(settings);
            var key = setting.getKey();
            var mappedKey = AZURE_TO_OPEN_DAL.get(key);
            assert mappedKey != null : "All known settings must have their OpenDAL counterpart specified.";
            if (value != null) {
                config.put(mappedKey, value);
            } else if (REQUIRED_SETTINGS.contains(key)) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Setting %s must be provided", key)
                );
            }
        }

        this.operator = AsyncOperator.of(NAME, config, sharedAsyncExecutor.asyncExecutor()).blocking();
    }

    @Override
    public OutputStream acquireOutputStream(Executor executor, URI uri, WriterProjection.CompressionType compressionType) throws IOException {
        OutputStream outputStream = operator.createOutputStream(uri.getPath());
        if (compressionType != null) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }

    @Override
    public void close() {
        assert operator != null : "Operator must be created before FileOutput is closed";
        operator.close();
    }

}
