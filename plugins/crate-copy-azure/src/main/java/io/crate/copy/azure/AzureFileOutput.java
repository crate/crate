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

import static io.crate.copy.azure.AzureCopyPlugin.NAME;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.zip.GZIPOutputStream;

import org.apache.opendal.Operator;
import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.NotNull;

import io.crate.execution.dsl.projection.WriterProjection;
import io.crate.execution.engine.export.FileOutput;

public class AzureFileOutput implements FileOutput {

    private final Map<String, String> config;

    public AzureFileOutput(Settings settings) {
        config = AzureBlobStorageSettings.openDALConfig(settings);
    }

    @Override
    public OutputStream acquireOutputStream(Executor executor, URI uri, WriterProjection.CompressionType compressionType) throws IOException {
        // Operator is closable (via parent NativeObject).
        // Closing acquired OutputStream is taken care of by the caller.
        // and closing OperatorOutputStream closes Operator (closes its NativeHandle).
        OutputStream outputStream =
            Operator.of(NAME, config).createOutputStream(azureResourcePath(uri, config.get("account_name")));
        if (compressionType != null) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }

    /**
     * Normalizes user provided URI (azblob:://path/to/dir) to the Azure compatible resource URI.
     * Resource URI must start with account name followed by /path/to/dir.
     * We cannot use uri.getPath() directly for URI looking like 'azblob::/path/to/dir', we extract directory on our own.
     */
    public static String azureResourcePath(URI uri, @NotNull String accountName) {
        return accountName + uri.toString().replace("azblob:/", "");
    }
}
