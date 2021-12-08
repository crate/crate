/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.export;

import io.crate.execution.dsl.projection.WriterProjection;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.zip.GZIPOutputStream;

public class LocalFsFileOutput implements FileOutput {

    @Override
    public OutputStream acquireOutputStream(Executor executor,
                                            URI uri,
                                            WriterProjection.CompressionType compressionType,
                                            Map<String, Object> withClauseOptions) throws IOException {
        if (uri.getHost() != null) {
            throw new IllegalArgumentException("the URI host must be defined");
        }
        String path = uri.getPath();
        File outFile = new File(path);
        if (outFile.exists()) {
            if (outFile.isDirectory()) {
                throw new IOException("Output path is a directory: " + path);
            }
        }
        OutputStream os = new FileOutputStream(outFile);
        if (compressionType != null) {
            os = new GZIPOutputStream(os);
        }
        return new BufferedOutputStream(os);
    }

    @Override
    public Set<String> validWithClauseOptions() {
        return Set.of();
    }
}
