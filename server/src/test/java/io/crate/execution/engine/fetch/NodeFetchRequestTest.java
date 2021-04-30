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

package io.crate.execution.engine.fetch;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.UUID;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

public class NodeFetchRequestTest {

    @Test
    public void testStreaming() throws Exception {

        IntObjectHashMap<IntArrayList> toFetch = new IntObjectHashMap<>();
        IntArrayList docIds = new IntArrayList(3);
        toFetch.put(1, docIds);

        NodeFetchRequest orig = new NodeFetchRequest(UUID.randomUUID(), 1, true, toFetch);

        BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);

        StreamInput in = out.bytes().streamInput();

        NodeFetchRequest streamed = new NodeFetchRequest(in);

        assertThat(orig.jobId(), is(streamed.jobId()));
        assertThat(orig.fetchPhaseId(), is(streamed.fetchPhaseId()));
        assertThat(orig.isCloseContext(), is(streamed.isCloseContext()));
        assertThat(orig.toFetch().toString(), is(streamed.toFetch().toString()));
    }
}
