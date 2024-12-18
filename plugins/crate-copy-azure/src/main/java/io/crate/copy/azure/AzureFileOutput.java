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

import static io.crate.copy.azure.AzureCopyPlugin.OPEN_DAL_SCHEME;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.zip.GZIPOutputStream;

import org.apache.opendal.AsyncOperator;
import org.apache.opendal.Operator;
import org.apache.opendal.OperatorOutputStream;
import org.elasticsearch.common.settings.Settings;

import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.export.FileOutput;
import sun.misc.Unsafe;

public class AzureFileOutput implements FileOutput {

    private final Map<String, String> config;
    private final Operator operator;
    private final String resourcePath;

    // Change hardcoded chunk size to a bigger value to avoid BlockCountExceedsLimit error.
    // TODO: Revert this once https://github.com/apache/opendal/issues/5421 is released.
    static {
        try {
            final Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);

            final Field maxBytesField = OperatorOutputStream.class.getDeclaredField("MAX_BYTES");
            //noinspection removal
            Object fieldBase = unsafe.staticFieldBase(maxBytesField);
            //noinspection removal
            long fieldOffset = unsafe.staticFieldOffset(maxBytesField);

            //noinspection removal
            unsafe.putObject(fieldBase, fieldOffset, 16384 * 10);
        } catch (Exception e) {
            // Do nothing, use hardcoded buffer size.
        }
    }

    public AzureFileOutput(URI uri, SharedAsyncExecutor sharedAsyncExecutor, Settings settings) {
        AzureURI azureURI = AzureURI.of(uri);
        this.config = OperatorHelper.config(azureURI, settings, false);
        this.resourcePath = azureURI.resourcePath();
        this.operator = AsyncOperator.of(OPEN_DAL_SCHEME, config, sharedAsyncExecutor.asyncExecutor()).blocking();
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
