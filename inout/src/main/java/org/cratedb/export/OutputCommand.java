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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;


/**
 * Start an OS Command as a process and push strings to the process'
 * standard in. Get standard out and standard error messages when
 * process has finished.
 */
public class OutputCommand extends Output {

    private static final int BUFFER_LEN = 8192;

    private final ProcessBuilder builder;
    private final boolean compression;
    private Process process;
    private Result result;
    private StreamConsumer outputConsumer, errorConsumer;
    private OutputStream os;

    /**
     * Initialize the process builder with a single command.
     * @param command
     */
    public OutputCommand(String command, boolean compression) {
        builder = new ProcessBuilder(command);
        this.compression = compression;
    }

    /**
     * Initialize the process with a command list.
     * @param cmdArray
     */
    public OutputCommand(List<String> cmdArray, boolean compression) {
        builder = new ProcessBuilder(cmdArray);
        this.compression = compression;
    }

    /**
     * Start the process and prepare writing to it's standard in.
     *
     * @throws IOException
     */
    public void open() throws IOException {
        process = builder.start();
        outputConsumer = new StreamConsumer(process.getInputStream(),
                BUFFER_LEN);
        errorConsumer = new StreamConsumer(process.getErrorStream(),
                BUFFER_LEN);
        os = process.getOutputStream();
        if (compression) {
            os = new GZIPOutputStream(os);
        }
    }

    /**
     * Get the output stream to write to the process' standard in.
     */
    public OutputStream getOutputStream() {
        return os;
    }

    /**
     * Stop writing to the process' standard in and wait until the
     * process is finished and close all resources.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (process != null) {
            os.flush();
            os.close();
            result = new Result();
            try {
                result.exit = process.waitFor();
            } catch (InterruptedException e) {
                result.exit = process.exitValue();
            }
            outputConsumer.waitFor();
            result.stdOut = outputConsumer.getBufferedOutput();
            errorConsumer.waitFor();
            result.stdErr = errorConsumer.getBufferedOutput();
        }
    }

    public Result result() {
        return result;
    }
}
