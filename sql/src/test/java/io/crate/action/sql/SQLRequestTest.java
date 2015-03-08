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
        request.creationTime = 0;
        request.includeTypesOnResponse(true);


        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        byte[] expectedBytes = new byte[]
                {0,19,115,101,108,101,99,116,32,42,32,102,114,111,109,32,117,115,101,114,115,2,0,4,97,114,103,49,0,4,97,114,103, 50, 0, 1};
        assertThat(out.bytes().toBytes(), is(expectedBytes));
    }


    @Test
    public void testSerializationReadFrom() throws Exception {
        byte[] buf = new byte[]
                {0,19,115,101,108,101,99,116,32,42,32,102,114,111,109,32,117,115,101,114,115,2,0,4,97,114,103,49,0,4,97,114,103, 50, 0, 1};
        BytesStreamInput in = new BytesStreamInput(buf, false);
        SQLRequest request = new SQLRequest();
        request.readFrom(in);

        assertThat(request.args(), is(new Object[] { "arg1", "arg2" }));
        assertThat(request.includeTypesOnResponse(), is(true));
        assertThat(request.stmt(), is("select * from users"));
    }
}