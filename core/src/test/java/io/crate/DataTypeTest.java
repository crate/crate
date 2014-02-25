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

package io.crate;

import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import static junit.framework.Assert.assertNull;
import static junit.framework.TestCase.assertEquals;

public class DataTypeTest {

    @Test
    public void testStreaming() throws Exception {

        BytesRef b1 = new BytesRef("hello");
        BytesStreamOutput out = new BytesStreamOutput();
        DataType.Streamer streamer = DataType.STRING.streamer();
        streamer.writeTo(out, b1);
        BytesStreamInput in = new BytesStreamInput(out.bytes());
        BytesRef b2 = (BytesRef) streamer.readFrom(in);
        assertEquals(b1, b2);

    }

    @Test
    public void testStreamingNull() throws Exception {

        BytesRef b1 = null;
        BytesStreamOutput out = new BytesStreamOutput();
        DataType.Streamer streamer = DataType.STRING.streamer();
        streamer.writeTo(out, b1);
        BytesStreamInput in = new BytesStreamInput(out.bytes());
        BytesRef b2 = (BytesRef) streamer.readFrom(in);
        assertNull(b2);

    }



}
