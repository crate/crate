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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.UUID;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectHashMap;

import io.crate.data.breaker.RamAccounting;

public class FetchRequestTest {

    @Test
    public void testStreaming() throws Exception {

        IntObjectHashMap<IntArrayList> toFetch = new IntObjectHashMap<>();
        IntArrayList docIds = new IntArrayList(3);
        toFetch.put(1, docIds);

        NodeFetchRequest.FetchRequest orig = new NodeFetchRequest(
            "dummy", UUID.randomUUID(), 1, true, toFetch, new IntObjectHashMap<>(), mock(RamAccounting.class)
        ).innerRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);

        StreamInput in = out.bytes().streamInput();

        NodeFetchRequest.FetchRequest streamed = new NodeFetchRequest.FetchRequest(in);

        assertThat(orig.jobId()).isEqualTo(streamed.jobId());
        assertThat(orig.fetchPhaseId()).isEqualTo(streamed.fetchPhaseId());
        assertThat(orig.isCloseContext()).isEqualTo(streamed.isCloseContext());
        assertThat(orig.toFetch().toString()).isEqualTo(streamed.toFetch().toString());
    }
}
