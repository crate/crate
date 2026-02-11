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

package io.crate.copy.s3;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import io.crate.execution.engine.collect.files.FileInputFactory;
import io.crate.execution.engine.export.FileOutputFactory;
import io.crate.opendal.SharedAsyncExecutor;
import io.crate.plugin.CopyPlugin;

public class S3CopyPlugin extends Plugin implements CopyPlugin {

    private final SharedAsyncExecutor sharedAsyncExecutor;

    @Inject
    public S3CopyPlugin(Settings settings) {
        this.sharedAsyncExecutor = SharedAsyncExecutor.getInstance(settings);
    }

    @Override
    public void close() throws IOException {
        sharedAsyncExecutor.close();
    }

    public Map<String, FileInputFactory> getFileInputFactories() {
        return Map.of(
            S3FileInputFactory.NAME,
            new S3FileInputFactory(sharedAsyncExecutor.asyncExecutor())
        );
    }

    public Map<String, FileOutputFactory> getFileOutputFactories() {
        return Map.of(
            S3FileOutputFactory.NAME,
            new S3FileOutputFactory(sharedAsyncExecutor.asyncExecutor())
        );
    }
}
