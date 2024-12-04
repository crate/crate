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

package io.crate.protocols.postgres.types;

import java.util.Map;

import org.junit.Test;

import io.crate.common.collections.MapBuilder;

public class JsonTypeTest extends BasePGTypeTest<Object> {

    private Map<String, Object> map = MapBuilder.<String, Object>newMapBuilder()
        .put("foo", "bar")
        .put("x", 10)
        .map();

    public JsonTypeTest() {
        super(JsonType.INSTANCE);
    }

    @Test
    public void testWriteValue() throws Exception {
        byte[] expectedBytes = new byte[]{
            0, 0, 0, 20, 123, 34, 102, 111, 111, 34, 58, 34, 98, 97, 114, 34, 44, 34, 120, 34, 58, 49, 48, 125
        };
        assertBytesWritten(map, expectedBytes, 24);
    }

    @Test
    public void testReadValue() throws Exception {
        byte[] bytes = new byte[]{
            123, 34, 102, 111, 111, 34, 58, 34, 98, 97, 114, 34, 44, 34, 120, 34, 58, 49, 48, 125
        };
        assertBytesReadBinary(bytes, map, 20);
    }
}
