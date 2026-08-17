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

public class VarianceStreamingTest {

    @Test
    public void test_variance_state_round_trips_through_streamer() throws Exception {
        Variance original = new Variance();
        original.increment(1_000_000_000_001.0d);
        original.increment(1_000_000_000_002.0d);
        original.increment(1_000_000_000_003.0d);

        Streamer<Variance> streamer = VarianceStateType.INSTANCE.streamer();
        BytesStreamOutput out = new BytesStreamOutput();
        streamer.writeValueTo(out, original);
        StreamInput in = out.bytes().streamInput();
        Variance restored = streamer.readValueFrom(in);

        assertThat(restored.result()).isEqualTo(original.result());
    }
}
