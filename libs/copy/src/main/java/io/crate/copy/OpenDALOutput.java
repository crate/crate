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

package io.crate.copy;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.zip.GZIPOutputStream;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.elasticsearch.common.settings.Settings;

import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.export.FileOutput;

public abstract class OpenDALOutput<T extends OpenDalURI> implements FileOutput {

    private final Operator operator;
    private final String resourcePath;

    public OpenDALOutput(String scheme,
                         T openDalURI,
                         Configuration<T> configuration,
                         SharedAsyncExecutor sharedAsyncExecutor,
                         Settings settings) {
        configuration.validate(settings, false);
        Map<String, String> config = configuration.fromURIAndSettings(openDalURI, settings);
        this.operator = AsyncOperator.of(scheme, config, sharedAsyncExecutor.asyncExecutor()).blocking();
        this.resourcePath = openDalURI.resourcePath();
    }

    @Override
    public OutputStream acquireOutputStream(Executor executor, WriterProjection.CompressionType compressionType) throws IOException {
        OutputStream outputStream = operator.createOutputStream(resourcePath);
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
