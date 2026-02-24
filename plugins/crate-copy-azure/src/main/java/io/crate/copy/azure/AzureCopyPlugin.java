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

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import io.crate.execution.engine.collect.files.FileInputFactory;
import io.crate.execution.engine.export.FileOutputFactory;
import io.crate.opendal.SharedAsyncExecutor;
import io.crate.plugin.CopyPlugin;

public class AzureCopyPlugin extends Plugin implements CopyPlugin {

    public static final String OPEN_DAL_SCHEME = "azblob";
    public static final String USER_FACING_SCHEME = "az";

    private final SharedAsyncExecutor sharedAsyncExecutor;

    @Inject
    public AzureCopyPlugin(Settings settings) {
        this.sharedAsyncExecutor = SharedAsyncExecutor.getInstance(settings);
    }

    @Override
    public void close() throws IOException {
        sharedAsyncExecutor.close();
    }

    public Map<String, FileInputFactory> getFileInputFactories() {
        return Map.of(USER_FACING_SCHEME, new AzureFileInputFactory(sharedAsyncExecutor));
    }

    public Map<String, FileOutputFactory> getFileOutputFactories() {
        return Map.of(USER_FACING_SCHEME, new AzureFileOutputFactory(sharedAsyncExecutor.asyncExecutor()));
    }
}
