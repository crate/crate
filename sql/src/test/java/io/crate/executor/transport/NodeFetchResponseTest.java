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
import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.google.common.collect.Iterables;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataTypes;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.MemoryCircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isRow;

public class NodeFetchResponseTest extends CrateUnitTest {

    private IntObjectMap<Streamer[]> streamers;
    private IntObjectMap<StreamBucket> fetched;
    private long originalFlushBufferSize = RamAccountingContext.FLUSH_BUFFER_SIZE;
    private RamAccountingContext ramAccountingContext = new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy"));

    @Before
    public void setUpStreamBucketsAndStreamer() throws Exception {
        originalFlushBufferSize = RamAccountingContext.FLUSH_BUFFER_SIZE;
        RamAccountingContext.FLUSH_BUFFER_SIZE = 2;
        streamers = new IntObjectHashMap<>(1);
        streamers.put(1, new Streamer[]{DataTypes.BOOLEAN.streamer()});

        IntObjectHashMap<IntContainer> toFetch = new IntObjectHashMap<>();
        IntHashSet docIds = new IntHashSet(3);
        toFetch.put(1, docIds);
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers.get(1), ramAccountingContext);
        builder.add(new RowN(new Object[]{true}));
        fetched = new IntObjectHashMap<>(1);
        fetched.put(1, builder.build());
    }

    @After
    public void reset() throws Exception {
        RamAccountingContext.FLUSH_BUFFER_SIZE = originalFlushBufferSize;
    }

    @Test
    public void testStreaming() throws Exception {
        NodeFetchResponse orig = NodeFetchResponse.forSending(fetched);

        BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);

        StreamInput in = out.bytes().streamInput();

        // receiving side is required to set the streamers
        NodeFetchResponse streamed = NodeFetchResponse.forReceiveing(streamers, ramAccountingContext);
        streamed.readFrom(in);

        assertThat((Row) Iterables.getOnlyElement(streamed.fetched().get(1)), isRow(true));
    }

    @Test
    public void testResponseCircuitBreaker() throws Exception {
        NodeFetchResponse orig = NodeFetchResponse.forSending(fetched);
        BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);
        StreamInput in = out.bytes().streamInput();

        NodeFetchResponse nodeFetchResponse = NodeFetchResponse.forReceiveing(
            streamers,
            new RamAccountingContext("test",
                new MemoryCircuitBreaker(
                    new ByteSizeValue(2, ByteSizeUnit.BYTES), 1.0, Loggers.getLogger(NodeFetchResponseTest.class))));

        expectedException.expect(CircuitBreakingException.class);
        nodeFetchResponse.readFrom(in);
    }
}
