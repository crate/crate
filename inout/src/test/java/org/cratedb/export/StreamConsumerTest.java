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

package org.cratedb.export;

import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for the @StreamConsumer class.
 */
public class StreamConsumerTest {

    @Test
    public void test() {
        // Prepare an input stream with more than 8 bytes.
        String tmp = "one\ntwo\nthree\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(tmp.getBytes());

        // Initialize a consumer. Remember the first 8 bytes.
        StreamConsumer consumer = new StreamConsumer(inputStream, 8);

        // Immediately the consumer does not get any output yet.
        String output = consumer.getBufferedOutput();
        assertEquals("", output);

        // Wait for the stream to finish.
        consumer.waitFor();

        // The output delivers the first 8 bytes of the stream.
        output = consumer.getBufferedOutput();
        assertEquals("one\ntwo\n", output);

        // The input stream has no bytes left. The rest of the output
        // is consumed.
        assertEquals(0, inputStream.available());
    }

}
