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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;

import io.crate.Streamer;
import io.crate.breaker.ConcurrentRamAccounting;
import io.crate.data.Row1;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.distribution.StreamBucket;
import io.crate.types.DataTypes;

public class NodeFetchResponseTest extends ESTestCase {

    private IntObjectMap<Streamer<?>[]> streamers;
    private IntObjectMap<StreamBucket> fetched;

    @Before
    public void setUpStreamBucketsAndStreamer() throws Exception {
        streamers = new IntObjectHashMap<>(1);
        streamers.put(1, new Streamer[]{DataTypes.BOOLEAN.streamer()});

        IntObjectHashMap<IntContainer> toFetch = new IntObjectHashMap<>();
        IntHashSet docIds = new IntHashSet(3);
        toFetch.put(1, docIds);
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers.get(1), RamAccounting.NO_ACCOUNTING);
        builder.add(new RowN(new Object[]{true}));
        fetched = new IntObjectHashMap<>(1);
        fetched.put(1, builder.build());
    }

    @Test
    public void testStreaming() throws Exception {
        NodeFetchResponse orig = new NodeFetchResponse(fetched);

        BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);

        StreamInput in = out.bytes().streamInput();

        // receiving side is required to set the streamers
        NodeFetchResponse streamed = new NodeFetchResponse(in, streamers, RamAccounting.NO_ACCOUNTING);

        assertThat(streamed.fetched().get(1)).hasSize(1);
        assertThat(streamed.fetched().get(1).iterator().next()).isEqualTo(new Row1(true));
    }

    @Test
    public void testResponseCircuitBreaker() throws Exception {
        NodeFetchResponse orig = new NodeFetchResponse(fetched);
        BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);
        StreamInput in = out.bytes().streamInput();

        expectedException.expect(CircuitBreakingException.class);
        new NodeFetchResponse(
            in,
            streamers,
            ConcurrentRamAccounting.forCircuitBreaker(
                "test",
                new MemoryCircuitBreaker(
                    new ByteSizeValue(2, ByteSizeUnit.BYTES),
                    1.0,
                    LogManager.getLogger(NodeFetchResponseTest.class)),
                0
            )
        );

    }
}
