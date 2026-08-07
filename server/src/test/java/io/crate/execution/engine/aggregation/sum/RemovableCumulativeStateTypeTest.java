/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.aggregation.sum;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.joda.time.Period;
import org.junit.Test;

import io.crate.Streamer;
import io.crate.types.DataTypes;

public class RemovableCumulativeStateTypeTest {

    @Test
    public void test_streaming_state_with_count_zero_has_null_value() throws Exception {
        RemovableCumulativeState<Long> state = new RemovableCumulativeState<>(null, DataTypes.LONG, 0);

        Streamer<RemovableCumulativeState<Long>> streamer = RemovableCumulativeStateType.INSTANCE.streamer();
        BytesStreamOutput out = new BytesStreamOutput();
        streamer.writeValueTo(out, state);
        StreamInput in = out.bytes().streamInput();
        RemovableCumulativeState<Long> stateFromStream = streamer.readValueFrom(in);

        assertThat(stateFromStream.count()).isEqualTo(0L);
        assertThat(stateFromStream.value()).isNull();
    }

    @Test
    public void test_streaming_numeric_state() throws Exception {
        RemovableCumulativeState<BigDecimal> state = new RemovableCumulativeState<>(BigDecimal.TEN, DataTypes.NUMERIC, 1);

        Streamer<RemovableCumulativeState<BigDecimal>> streamer = RemovableCumulativeStateType.INSTANCE.streamer();
        BytesStreamOutput out = new BytesStreamOutput();
        streamer.writeValueTo(out, state);
        StreamInput in = out.bytes().streamInput();
        RemovableCumulativeState<BigDecimal> stateFromStream = streamer.readValueFrom(in);

        assertThat(stateFromStream.count()).isEqualTo(1);
        assertThat(stateFromStream.value()).isEqualTo(BigDecimal.valueOf(10));
    }

    @Test
    public void test_streaming_interval_state() throws Exception {
        RemovableCumulativeState<Period> state = new RemovableCumulativeState<>(Period.days(1), DataTypes.INTERVAL, 1);

        Streamer<RemovableCumulativeState<Period>> streamer = RemovableCumulativeStateType.INSTANCE.streamer();
        BytesStreamOutput out = new BytesStreamOutput();
        streamer.writeValueTo(out, state);
        StreamInput in = out.bytes().streamInput();
        RemovableCumulativeState<Period> stateFromStream = streamer.readValueFrom(in);

        assertThat(stateFromStream.count()).isEqualTo(1);
        assertThat(stateFromStream.value()).isEqualTo(Period.days(1));
    }
}
