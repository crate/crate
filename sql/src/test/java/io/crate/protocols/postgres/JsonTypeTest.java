/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.protocols.postgres;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class JsonTypeTest {

    private PGType.JsonType jsonType;
    private Map<String, Object> map;

    @Before
    public void setUp() throws Exception {
        jsonType = new PGType.JsonType();

        map = new HashMap<>();
        map.put("foo", "bar");
        map.put("x", 10);
    }

    @Test
    public void writeTextValue() throws Exception {
        ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
        int bytesWritten = jsonType.writeTextValue(buffer, map);
        assertThat(bytesWritten, is(24));
        byte[] bytes = new byte[24];
        buffer.getBytes(0, bytes);
        byte[] expectedBytes = new byte[]{
            0, 0, 0, 20, 123, 34, 102, 111, 111, 34, 58, 34, 98, 97, 114, 34, 44, 34, 120, 34, 58, 49, 48, 125
        };
        assertThat(bytes, is(expectedBytes));
    }

    @Test
    public void readTextValue() throws Exception {
        String json = "{\"foo\":\"bar\",\"x\":10}";
        ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(json.getBytes());
        Map<String, Object> value = (Map<String, Object>) jsonType.readTextValue(buffer, json.length());
        assertThat(value, is(map));
    }

}
