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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.export;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

public class OutputFile extends Output {

    private Result result;
    private final String path;
    private OutputStream os;
    private final boolean overwrite;
    private final boolean compression;

    public OutputFile(String path, boolean overwrite, boolean compression) {
        this.path = path;
        this.overwrite = overwrite;
        this.compression = compression;
    }

    @Override
    public void open() throws IOException {
        File outFile = new File(path);
        if (!overwrite && outFile.exists()){
            throw new IOException("File exists: " +  path);
        }
        os = new FileOutputStream(outFile);
        if (compression) {
            os = new GZIPOutputStream(os);
        }
    }

    @Override
    public void close() throws IOException {
        result = new Result();
        if (os != null) {
            os.close();
            result.exit = 0;
        } else {
            result.exit = 1;
        }
        os = null;
    }

    @Override
    public OutputStream getOutputStream() {
        return os;
    }

    @Override
    public Result result() {
        return result;
    }
}
