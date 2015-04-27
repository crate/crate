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

import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class BulkCreateIndicesResponseTest {

    @Test
    public void testSerializationEmpty() throws Exception {
        BulkCreateIndicesResponse response = new BulkCreateIndicesResponse();
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());

        BulkCreateIndicesResponse responseDeserialized = new BulkCreateIndicesResponse();
        responseDeserialized.readFrom(in);

        assertThat(responseDeserialized.responses(), is(empty()));
        assertThat(responseDeserialized.isAcknowledged(), is(false));
    }

    @Test
    public void testSerialization() throws Exception {
        BulkCreateIndicesResponse response = new BulkCreateIndicesResponse(
                Arrays.asList(new CreateIndexResponse(true), new CreateIndexResponse(false))
        );
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(out.bytes());

        BulkCreateIndicesResponse responseDeserialized = new BulkCreateIndicesResponse();
        responseDeserialized.readFrom(in);

        assertThat(responseDeserialized.responses().size(), is(2));
        assertThat(responseDeserialized.isAcknowledged(), is(false));
    }
}
