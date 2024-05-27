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

package io.crate.execution.engine.distribution;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;
import io.crate.types.DataTypes;

public class DistributedResultRequestTest extends ESTestCase {

    @Test
    public void testStreaming() throws Exception {
        Streamer<?>[] streamers = new Streamer[]{DataTypes.STRING.streamer()};

        UUID uuid = UUID.randomUUID();
        StreamBucket.Builder builder = new StreamBucket.Builder(streamers, RamAccounting.NO_ACCOUNTING);
        builder.add(new RowN(new Object[] {"ab"}));
        builder.add(new RowN(new Object[] {null}));
        builder.add(new RowN(new Object[] {"cd"}));
        DistributedResultRequest r1 =
            DistributedResultRequest.of(
                "dummyNodeId", uuid, 1, (byte) 3, 1, builder.build(), false
            ).innerRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        r1.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        DistributedResultRequest r2 = new DistributedResultRequest(in);

        assertThat(r2.readRows(streamers).size()).isEqualTo(r1.readRows(streamers).size());
        assertThat(r1.isLast()).isEqualTo(r2.isLast());
        assertThat(r1.executionPhaseInputId()).isEqualTo(r2.executionPhaseInputId());

        Bucket result = r2.readRows(streamers);
        List<Object[]> rows = StreamSupport.stream(result.spliterator(), false)
            .map(Row::materialize)
            .toList();
        assertThat(rows).containsExactly(
            new Object[] {"ab"},
            new Object[] {null},
            new Object[] {"cd"}
        );
    }

    @Test
    public void testStreamingOfFailure() throws Exception {
        UUID uuid = UUID.randomUUID();
        Throwable throwable = new IllegalStateException("dummy");

        DistributedResultRequest r1 =
            new DistributedResultRequest.Builder(uuid, 1, (byte) 3, 1, throwable, true)
                .build("dummyNodeId")
                .innerRequest();

        BytesStreamOutput out = new BytesStreamOutput();
        r1.writeTo(out);
        StreamInput in = StreamInput.wrap(BytesReference.toBytes(out.bytes()));
        DistributedResultRequest r2 = new DistributedResultRequest(in);

        assertThat(r2.throwable()).isExactlyInstanceOf(throwable.getClass());
        assertThat(r2.isKilled()).isEqualTo(r1.isKilled());
    }
}
