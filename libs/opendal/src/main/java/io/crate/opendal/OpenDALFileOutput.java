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

package io.crate.opendal;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executor;
import java.util.zip.GZIPOutputStream;

import org.apache.opendal.Operator;

import io.crate.execution.dsl.projection.WriterProjection.CompressionType;
import io.crate.execution.engine.export.FileOutput;

public class OpenDALFileOutput implements FileOutput {

    // 256 KiB, 16x size of the OperatorOutputStream's default buffer size.
    // Helps to avoid uploading too many chunks (which causes BlockCountExceedsLimit error on azure).
    // This also speeds up file upload
    private static final int MAX_BYTES = 262144;

    private final Operator operator;
    private final String path;

    /// @param operator OpenDAL operator. Is closed when the FileInput is closed
    public OpenDALFileOutput(Operator operator, String path) {
        this.operator = operator;
        this.path = path;
    }

    @Override
    public OutputStream acquireOutputStream(Executor executor, CompressionType compressionType) throws IOException {
        OutputStream outputStream = operator.createOutputStream(path, MAX_BYTES);
        if (compressionType != null) {
            outputStream = new GZIPOutputStream(outputStream);
        }
        return outputStream;
    }

    @Override
    public void close() {
        operator.close();
    }
}
