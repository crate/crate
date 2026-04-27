/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.aggregation;

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Before;
import org.junit.Test;

import io.crate.data.breaker.RamAccounting;
import io.crate.operation.aggregation.LTTBAggregation.LttbState;
import io.crate.testing.PlainRamAccounting;

public class LttbStateTest extends AggregationTestCase {

    @Before
    public void prepareFunctions() {
        nodeCtx = createNodeContext(null, null);
    }

    @Test
    public void test_should_reconstruct_state_when_streamed_from_one_state_to_another() throws Exception {
        LttbState state1 = new LttbState();
        RamAccounting ramAccounting = new PlainRamAccounting();
        state1.init(ramAccounting, 10);
        state1.addPoint(ramAccounting, 20L, 22.0);

        BytesStreamOutput out = new BytesStreamOutput();
        state1.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LttbState state2 = new LttbState(in);

        assertThat(state1.getThreshold()).isEqualTo(state2.getThreshold());
        assertThat(state1.isInitialized()).isEqualTo(state2.isInitialized());

        for (int i = 0; i < state1.getPoints().size(); i++) {
            assertThat(state1.getPoints().get(i).timestamp()).isEqualTo(state2.getPoints().get(i).timestamp());
            assertThat(state1.getPoints().get(i).value()).isEqualTo(state2.getPoints().get(i).value());
        }
    }

    @Test
    public void test_init_should_account_for_memory() throws Exception {
        LttbState state = new LttbState();
        RamAccounting ramAccounting = new PlainRamAccounting();
        state.init(ramAccounting, 10);
        assertThat(ramAccounting.totalBytes()).isEqualTo(RamUsageEstimator.shallowSizeOfInstance(ArrayList.class));
    }

    @Test
    public void test_addPoint_should_account_for_memory() throws Exception {
        LttbState state = new LttbState();
        RamAccounting ramAccounting = new PlainRamAccounting();
        state.init(ramAccounting, 10);
        Long ramBefore = ramAccounting.totalBytes();
        state.addPoint(ramAccounting, 1L, 0.1);
        state.addPoint(ramAccounting, 2L, 0.1);
        state.addPoint(ramAccounting, 3L, 0.1);
        assertThat(ramAccounting.totalBytes())
                .isEqualTo(ramBefore + LTTBAggregation.Point.SHALLOW_SIZE * 3);
    }

}
