/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.action.sql;

import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class SQLRequestTest {


    @Test
    public void testBulkArgsSerialization() throws Exception {
        SQLRequest request = new SQLRequest(
                "select * from sys.cluster",
                new Object[][] {
                        new Object[] { "dummy", "args" },
                        new Object[] { "more", "args" }
                });

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());
        SQLRequest serialized = new SQLRequest();
        serialized.readFrom(in);

        assertArrayEquals(request.bulkArgs(), serialized.bulkArgs());
    }

    @Test
    public void testEmptyBulkArgsSerialization() throws Exception {
        SQLRequest request = new SQLRequest("select * from sys.cluster");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());
        SQLRequest serialized = new SQLRequest();
        serialized.readFrom(in);

        Object[][] empty = new Object[0][];
        assertArrayEquals(empty, serialized.bulkArgs());
    }
}