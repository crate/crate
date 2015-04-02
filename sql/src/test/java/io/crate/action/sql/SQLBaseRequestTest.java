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

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class SQLBaseRequestTest extends CrateUnitTest {

    @Test
    public void testBulkArgsSerialization() throws Exception {
        SQLBulkRequest request = new SQLBulkRequest(
                "select * from sys.cluster",
                new Object[][] {
                        new Object[] { "dummy", "args" },
                        new Object[] { "more", "args" }
                });

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());
        SQLBulkRequest serialized = new SQLBulkRequest();
        serialized.readFrom(in);

        assertArrayEquals(request.bulkArgs(), serialized.bulkArgs());
    }

    @Test
    public void testEmptyBulkArgsSerialization() throws Exception {
        SQLBulkRequest request = new SQLBulkRequest("select * from sys.cluster");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());
        SQLBulkRequest serialized = new SQLBulkRequest();
        serialized.readFrom(in);

        Object[][] empty = new Object[0][];
        assertArrayEquals(empty, serialized.bulkArgs());
    }

    @Test
    public void testResetSchemaName() throws Exception {
        SQLRequest request = new SQLRequest("select * from sys.cluster");
        request.setDefaultSchema(null);

        assertThat(request.getDefaultSchema(), Matchers.nullValue());
        assertThat(request.hasHeader("_s"), is(false));

        request.setDefaultSchema("foo");
        assertThat(request.getDefaultSchema(), is("foo"));

        request.setDefaultSchema(null);
        assertThat(request.getDefaultSchema(), Matchers.nullValue());
        assertThat(request.hasHeader("_s"), is(true));
    }
}