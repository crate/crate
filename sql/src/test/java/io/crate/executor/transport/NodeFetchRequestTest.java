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

package io.crate.executor.transport;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.IntOpenHashSet;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class NodeFetchRequestTest {

    @Test
    public void testStreaming() throws Exception {

        IntObjectOpenHashMap<IntContainer> toFetch = new IntObjectOpenHashMap<>();
        IntOpenHashSet docIds = new IntOpenHashSet(3);
        toFetch.put(1, docIds);

        NodeFetchRequest orig = new NodeFetchRequest(UUID.randomUUID(), 1, toFetch);

        BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);

        BytesStreamInput in = new BytesStreamInput(out.bytes());

        NodeFetchRequest streamed = new NodeFetchRequest();
        streamed.readFrom(in);


        assertThat(orig.jobId(), is(streamed.jobId()));
        assertThat(orig.fetchPhaseId(), is(streamed.fetchPhaseId()));
        assertThat(orig.toFetch().toString(), is(streamed.toFetch().toString()));
    }
}