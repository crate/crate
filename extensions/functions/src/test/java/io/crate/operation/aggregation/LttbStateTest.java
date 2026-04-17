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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Before;
import org.junit.Test;

import io.crate.operation.aggregation.LargestTriangleThreeBucketsAggregation.LttbState;
import io.crate.operation.aggregation.LargestTriangleThreeBucketsAggregation.Point;

public class LttbStateTest extends AggregationTestCase {

    @Before
    public void prepareFunctions() {
        nodeCtx = createNodeContext(null, null);
    }

    @Test
    public void test_should_sort_x_value_when_downsampling() throws Exception {
        LttbState state = new LttbState();
        state.init(memoryManager, 3);

        state.addPoint(3L, 30.0);
        state.addPoint(2L, 20.0);
        state.addPoint(1L, 10.0);

        List<Point> result = state.downsample();
        assertThat(result).extracting(i -> i.timestamp.longValue()).containsExactly(1L, 2L, 3L);
    }

    @Test
    public void test_should_downsample_to_threshold_number_of_points() throws Exception {
        LttbState state = new LttbState();
        state.init(memoryManager, 3);

        state.addPoint(10L, 100.0);
        state.addPoint(9L, 20.0);
        state.addPoint(8L, 5.0);
        state.addPoint(7L, 10.0);
        state.addPoint(6L, 50.0);
        state.addPoint(5L, 20.0);
        state.addPoint(4L, 10.0);
        state.addPoint(3L, 20.0);
        state.addPoint(2L, 80.0);
        state.addPoint(1L, 90.0);

        assertThat(state.getPoints()).hasSize(10);

        List<Point> result = state.downsample();
        assertThat(result).extracting(i -> i.timestamp).hasSize(3);
    }

    @Test
    public void test_should_select_the_points_that_form_largest_area_when_downsampling() throws Exception {
        LttbState state = new LttbState();
        state.init(memoryManager, 3);

        state.addPoint(1L, 10.0);
        state.addPoint(2L, 12.0);
        state.addPoint(3L, 50.0);
        state.addPoint(4L, 15.0);
        state.addPoint(5L, 11.0);

        List<Point> result = state.downsample();
        assertThat(result).extracting(i -> i.timestamp.longValue()).containsExactly(1L, 3L, 5L);
        assertThat(result).extracting(i -> i.value.doubleValue()).containsExactly(10.0, 50.0, 11.0);
    }

    @Test
    public void test_should_select_the_points_that_form_largest_area_negative_datapoints_when_downsampling()
            throws Exception {
        LttbState state = new LttbState();
        state.init(memoryManager, 3);

        state.addPoint(1L, -10.0);
        state.addPoint(2L, 12.0);
        state.addPoint(3L, 50.0);
        state.addPoint(4L, 15.0);
        state.addPoint(5L, 11.0);

        List<Point> result = state.downsample();
        assertThat(result).extracting(i -> i.timestamp.longValue()).containsExactly(1L, 3L, 5L);
        assertThat(result).extracting(i -> i.value.doubleValue()).containsExactly(-10.0, 50.0, 11.0);
    }

    @Test
    public void test_should_not_downsample_rather_return_original_points_if_threshold_zero() throws Exception {
        LttbState state = new LttbState();
        state.init(memoryManager, 0);

        state.addPoint(5L, 20.0);
        state.addPoint(4L, 10.0);
        state.addPoint(3L, 20.0);
        state.addPoint(2L, 80.0);
        state.addPoint(1L, 90.0);

        assertThat(state.getPoints()).hasSize(5);

        List<Point> result = state.downsample();
        assertThat(result).extracting(i -> i.timestamp.longValue()).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(result).extracting(i -> i.value.doubleValue()).containsExactly(90.0, 80.0, 20.0, 10.0, 20.0);
    }

    @Test
    public void test_should_not_downsample_rather_return_original_points_if_threshold_larger_than_amount_of_data()
            throws Exception {
        LttbState state = new LttbState();
        state.init(memoryManager, 10);

        state.addPoint(5L, 20.0);
        state.addPoint(4L, 10.0);
        state.addPoint(3L, 20.0);
        state.addPoint(2L, 80.0);
        state.addPoint(1L, 90.0);

        assertThat(state.getPoints()).hasSize(5);

        List<Point> result = state.downsample();
        assertThat(result).extracting(i -> i.timestamp.longValue()).containsExactly(1L, 2L, 3L, 4L, 5L);
        assertThat(result).extracting(i -> i.value.doubleValue()).containsExactly(90.0, 80.0, 20.0, 10.0, 20.0);
    }

    @Test
    public void test_should_reconstruct_state_when_streamed_from_one_state_to_another() throws Exception {
        LttbState state1 = new LttbState();
        state1.init(memoryManager, 10);
        state1.addPoint(20L, 22.0);

        BytesStreamOutput out = new BytesStreamOutput();
        state1.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LttbState state2 = new LttbState(in);

        assertThat(state1.getThreshold()).isEqualTo(state2.getThreshold());
        assertThat(state1.isInitialized()).isEqualTo(state2.isInitialized());

        for (int i = 0; i < state1.getPoints().size(); i++) {
            assertThat(state1.getPoints().get(i).timestamp).isEqualTo(state2.getPoints().get(i).timestamp);
            assertThat(state1.getPoints().get(i).value).isEqualTo(state2.getPoints().get(i).value);
        }
    }

    @Test
    public void test_should_merge_two_states() throws Exception {
        LttbState state1 = new LttbState();
        state1.init(memoryManager, 10);
        state1.addPoint(20L, 22.0);

        LttbState state2 = new LttbState();
        state2.init(memoryManager, 10);
        state2.addPoint(50L, 1.0);

        state1.merge(state2);

        assertThat(state1.getPoints()).extracting(i -> i.timestamp).containsExactly(20L, 50L);
        assertThat(state1.getPoints()).extracting(i -> i.value).containsExactly(22.0, 1.0);
    }

    @Test
    public void test_raises_during_merge_if_states_are_inconsistent() throws Exception {
        LttbState state1 = new LttbState();
        state1.init(memoryManager, 10);
        state1.addPoint(20L, 22.0);

        LttbState state2 = new LttbState();
        state2.init(memoryManager, 200); // threshold does not match state1
        state2.addPoint(50L, 1.0);

        assertThatThrownBy(() -> state1.merge(state2))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("The thresholds between the two LttbStates do not match");
    }

    @Test
    public void test_init_should_succeed_if_not_already_initialized() throws Exception {
        LttbState state = new LttbState();
        state.init(memoryManager, 10);

        assertThat(state.getPoints()).isNotNull();
        assertThat(state.getThreshold()).isEqualTo(10);
        assertThat(state.isInitialized()).isTrue();
    }

    @Test
    public void test_init_raises_if_already_initialized() throws Exception {
        LttbState state = new LttbState();
        state.init(memoryManager, 10);

        assertThatThrownBy(() -> state.init(memoryManager, 01))
                .isExactlyInstanceOf(AssertionError.class)
                .hasMessage("LttbState was already initialized");
    }

}
