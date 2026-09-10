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

package io.crate.execution.engine.aggregation.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.execution.engine.aggregation.impl.VarianceAggregation.VarianceStateType;
import io.crate.execution.engine.aggregation.impl.VarianceAggregation.VarianceStateTypeWelford;

public class VarianceStreamingTest {

    @Test
    public void test_welford_state_round_trips_through_streamer() throws Exception {
        Variance original = new Variance(false);
        original.increment(1_000_000_000_001.0d);
        original.increment(1_000_000_000_002.0d);
        original.increment(1_000_000_000_003.0d);

        Variance restored = roundTrip(VarianceStateTypeWelford.INSTANCE.streamer(), original);

        assertThat(restored.isLegacy()).isFalse();
        assertThat(restored.result()).isEqualTo(original.result());
    }

    @Test
    public void test_legacy_state_round_trips_through_streamer() throws Exception {
        Variance original = new Variance(true);
        original.increment(10.7d);
        original.increment(42.9d);
        original.increment(0.3d);

        Variance restored = roundTrip(VarianceStateType.INSTANCE.streamer(), original);

        assertThat(restored.isLegacy()).isTrue();
        assertThat(restored.result()).isEqualTo(original.result());
    }

    private static Variance roundTrip(Streamer<Variance> streamer, Variance original) throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        streamer.writeValueTo(out, original);
        StreamInput in = out.bytes().streamInput();
        return streamer.readValueFrom(in);
    }
}
