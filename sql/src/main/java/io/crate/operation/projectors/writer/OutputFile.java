/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors.writer;

import io.crate.planner.projection.WriterProjection;
import com.google.common.base.Preconditions;
import org.elasticsearch.common.settings.Settings;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.zip.GZIPOutputStream;

public class OutputFile extends Output {

    private final String path;
    private final boolean overwrite;
    private final boolean compression;

    public OutputFile(URI uri, WriterProjection.CompressionType compressionType) {
        Preconditions.checkArgument(uri.getHost() == null);
        this.path = uri.getPath();
        compression = compressionType != null;
        this.overwrite = true;
    }

    @Override
    public OutputStream acquireOutputStream() throws IOException {
        File outFile = new File(path);
        if (outFile.exists()){
            if (!overwrite){
                throw new IOException("File exists: " + path);
            }
            if (outFile.isDirectory()){
                throw new IOException("Output path is a directory: " + path);
            }
        }
        OutputStream os = new FileOutputStream(outFile);
        if (compression) {
            os = new GZIPOutputStream(os);
        }
        return os;
    }
}
