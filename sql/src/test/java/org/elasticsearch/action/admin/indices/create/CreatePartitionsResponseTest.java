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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class CreatePartitionsResponseTest {

    @Test
    public void testSerializationNotAcknowledged() throws Exception {
        serializeAndAssertAcknowledged(false);
    }

    @Test
    public void testSerializationAcknowledged() throws Exception {
        serializeAndAssertAcknowledged(true);
    }

    private void serializeAndAssertAcknowledged(boolean acknowledged) throws IOException {
        CreatePartitionsResponse response = new CreatePartitionsResponse(acknowledged);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput in = out.bytes().streamInput();

        CreatePartitionsResponse responseDeserialized = new CreatePartitionsResponse();
        responseDeserialized.readFrom(in);

        assertThat(responseDeserialized.isAcknowledged(), is(acknowledged));
    }
}
