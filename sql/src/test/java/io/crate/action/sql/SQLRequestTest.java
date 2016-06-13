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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class SQLRequestTest extends CrateUnitTest {

    /**
     * the serialization tests here with fixed bytes arrays ensure that backward-compatibility isn't broken.
     */
    @Test
    public void testSerializationWriteTo() throws Exception {
        SQLRequest request = new SQLRequest(
                "select * from users",
                new Object[] { "arg1", "arg2" }
        );
        request.fetchProperties(new FetchProperties(20, true));
        request.includeTypesOnResponse(true);

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        byte[] expectedBytes = new byte[]
            {0, 19, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 117, 115, 101, 114, 115, 1, 20, 0, 0, 0, 6, -4, 35, -84, 0, 1, 2, 0, 4, 97, 114, 103, 49, 0, 4, 97, 114, 103, 50};
        assertThat(out.bytes().toBytes(), is(expectedBytes));
    }


    @Test
    public void testSerializationReadFrom() throws Exception {
        byte[] buf = new byte[]
            {0, 19, 115, 101, 108, 101, 99, 116, 32, 42, 32, 102, 114, 111, 109, 32, 117, 115, 101, 114, 115, 1, 20, 0, 0, 0, 6, -4, 35, -84, 0, 1, 2, 0, 4, 97, 114, 103, 49, 0, 4, 97, 114, 103, 50};
        StreamInput in = StreamInput.wrap(buf);
        SQLRequest request = new SQLRequest();
        request.readFrom(in);

        assertThat(request.args(), is(new Object[] { "arg1", "arg2" }));
        assertThat(request.includeTypesOnResponse(), is(true));
        assertThat(request.stmt(), is("select * from users"));
        FetchProperties fetchProperties = request.fetchProperties();
        assertThat(fetchProperties.fetchSize(), is(20));
        assertThat(fetchProperties.closeContext(), is(true));
    }

    @Test
    public void testSerializationWithHeadersSet() throws Exception {
        SQLRequest request = new SQLRequest("select * from users");
        request.setDefaultSchema("foo");

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        SQLRequest inRequest = new SQLRequest();
        inRequest.readFrom(in);

        assertThat(inRequest.stmt(), is("select * from users"));
        assertThat(inRequest.getDefaultSchema(), is("foo"));
    }
}
