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

package io.crate.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * A stream consumer to consume the output of an input stream.
 */
public class StreamConsumer {

    private final StreamConsumerImpl impl;
    private Thread thread;

    /**
     * Constructor.
     *
     * @param inputStream the input stream to consume
     * @param bufferSize the buffer size to keep (first x bytes)
     */
    public StreamConsumer(InputStream inputStream, int bufferSize) {
        impl = new StreamConsumerImpl(inputStream, bufferSize);
        thread = new Thread(impl);
        thread.start();
    }

    /**
     * Get the buffered output of the stream (first x bytes defined
     * in buffer size)
     * @return
     */
    public String getBufferedOutput() {
        return impl.getOutput();
    }

    /**
     * Wait for the stream to finish.
     */
    public void waitFor() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Implementation class for thread.
     */
    private final class StreamConsumerImpl implements Runnable {

        private final int bufferSize;
        private final StringBuffer collectedOutput = new StringBuffer();
        private InputStream inputStream;

        private StreamConsumerImpl(InputStream inputStream, int bufferSize) {
            this.bufferSize = bufferSize;
            this.inputStream = inputStream;
        }

        public void run() {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                    inputStream));
            String line;
            try {
                do {
                    line = bufferedReader.readLine();
                    if (line != null && collectedOutput.length() < bufferSize) {
                        collectedOutput.append(line + "\n");
                    }
                } while (line != null);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private String getOutput() {
            return collectedOutput.toString();
        }
    }
}
