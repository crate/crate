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

package io.crate.execution.engine.aggregation.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.stream.IntStream;

import org.assertj.core.data.Offset;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import com.tdunning.math.stats.Centroid;

import io.crate.Streamer;

public class TDigestStateTest {

    @Test
    public void testStreaming() throws Exception {
        TDigestState digestState1 = new TDigestState(250, new double[]{0.5, 0.8});
        digestState1.add(1.1);
        digestState1.add(2.2);
        digestState1.add(3.3);
        IntStream.range(0, 128).forEach(i -> digestState1.add(4.4));
        BytesStreamOutput out = new BytesStreamOutput();
        TDigestStateType digestStateType = TDigestStateType.INSTANCE;
        Streamer<TDigestState> streamer = digestStateType.streamer();
        streamer.writeValueTo(out, digestState1);
        StreamInput in = out.bytes().streamInput();
        TDigestState digestState2 = streamer.readValueFrom(in);

        assertThat(digestState1.compression()).isEqualTo(digestState2.compression(), Offset.offset(0.001d));
        assertThat(digestState1.fractions()[0]).isEqualTo(digestState2.fractions()[0], Offset.offset(0.001d));
        assertThat(digestState1.fractions()[1]).isEqualTo(digestState2.fractions()[1], Offset.offset(0.001d));

        // T-digest does not guarantee the same collection of centroids after serialization.
        // This simple test happens to produce matching centroids, allowing a sanity-check
        // of the streaming logic.
        var c1 = digestState1.centroids();
        var c2 = digestState2.centroids();
        assertThat(c1.size()).isEqualTo(c2.size());
        Iterator<Centroid> it1 = c1.iterator();
        Iterator<Centroid> it2 = c2.iterator();
        while (it1.hasNext()) {
            Centroid a = it1.next();
            Centroid b = it2.next();
            assertThat(a.mean()).isEqualTo(b.mean());
            assertThat(a.count()).isEqualTo(b.count());
        }
    }

    @Test
    public void testStreaming_on_or_before_6_0_4() throws Exception {
        TDigestState digestState1 = new TDigestState(250, new double[]{0.5, 0.8});
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_6_0_4);
        TDigestStateType digestStateType = TDigestStateType.INSTANCE;
        Streamer<TDigestState> streamer = digestStateType.streamer();
        streamer.writeValueTo(out, digestState1);
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_6_0_4);
        TDigestState digestState2 = streamer.readValueFrom(in);

        assertThat(digestState1.compression()).isEqualTo(digestState2.compression(), Offset.offset(0.001d));
        assertThat(digestState1.fractions()[0]).isEqualTo(digestState2.fractions()[0], Offset.offset(0.001d));
        assertThat(digestState1.fractions()[1]).isEqualTo(digestState2.fractions()[1], Offset.offset(0.001d));
    }
}
