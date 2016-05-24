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

import com.google.common.collect.Iterables;
import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntHashSet;
import io.crate.Streamer;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isRow;
import static org.junit.Assert.assertThat;

public class NodeFetchResponseTest {

    @Test
    public void testStreaming() throws Exception {

        IntObjectHashMap<IntContainer> toFetch = new IntObjectHashMap<>();
        IntHashSet docIds = new IntHashSet(3);
        toFetch.put(1, docIds);

        IntObjectMap<Streamer[]> streamers = new IntObjectHashMap<>(1);
        streamers.put(1, new Streamer[]{DataTypes.BOOLEAN.streamer()});

        StreamBucket.Builder builder = new StreamBucket.Builder(streamers.get(1));
        builder.add(new RowN(new Object[]{true}));
        IntObjectHashMap<StreamBucket> fetched = new IntObjectHashMap<>(1);
        fetched.put(1, builder.build());

        NodeFetchResponse orig = NodeFetchResponse.forSending(fetched);

        BytesStreamOutput out = new BytesStreamOutput();
        orig.writeTo(out);


        StreamInput in = StreamInput.wrap(out.bytes());

        // receiving side is required to set the streamers
        NodeFetchResponse streamed = NodeFetchResponse.forReceiveing(streamers);
        streamed.readFrom(in);

        assertThat((Row) Iterables.getOnlyElement(streamed.fetched().get(1)), isRow(true));
    }
}
