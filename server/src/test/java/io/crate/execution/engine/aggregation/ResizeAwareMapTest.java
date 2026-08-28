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

package io.crate.execution.engine.aggregation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.RamUsageEstimator;
import org.junit.Test;

import io.crate.types.DataTypes;
import io.netty.util.collection.IntObjectHashMap;

public class ResizeAwareMapTest {

    @Test
    public void test_wrapperForJDKMap_uses_correct_defaults() {
        Map<Integer, Integer> map = new HashMap<>();
        ResizeAwareMap<Integer, Integer> resizeAwareMap = GroupByMaps.wrapperForJDKMap(map);
        int defaultCapacity = 16; // HashMap.DEFAULT_INITIAL_CAPACITY
        float defaultLoadFactor = 0.75f; // HashMap.DEFAULT_LOAD_FACTOR
        int threshold = (int) (defaultCapacity * defaultLoadFactor);
        assertThat(resizeAwareMap.currentCapacity()).isEqualTo(16); //HashMap. DEFAULT_INITIAL_CAPACITY
        for (int i = 0; i < threshold; i++) {
            assertThat(resizeAwareMap.expectedCapacityIncrease()).isEqualTo(0);
            map.put(i, i);
        }
    }

    @Test
    public void test_jdk_map_accounts_only_object_ref_per_slot() {
        Map<Integer, Integer> map = new HashMap<>();
        ResizeAwareMap<Integer, Integer> resizeAwareMap = GroupByMaps.wrapperForJDKMap(map);
        int defaultCapacity = 16; // HashMap.DEFAULT_INITIAL_CAPACITY
        float defaultLoadFactor = 0.75f; // HashMap.DEFAULT_LOAD_FACTOR
        int threshold = (int) (defaultCapacity * defaultLoadFactor);
        int prevCapacity = resizeAwareMap.currentCapacity();
        for (int i = 0; i < threshold ; i++) {
            resizeAwareMap.put(i, i);
        }

        // Get prediction before put
        long predictedBytes = resizeAwareMap.expectedCapacityIncreaseBytes();
        resizeAwareMap.put(threshold, threshold); // trigger resize
        int newCapacity = resizeAwareMap.currentCapacity();

        long deltaInBytes = (long) (newCapacity - prevCapacity) * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        assertThat(predictedBytes).isEqualTo(deltaInBytes);
    }

    @Test
    public void test_netty_map_accounts_object_ref_and_primitive_per_slot() {
        ResizeAwareMap<Integer, Object> resizeAwareMap = GroupByMaps.mapForType(DataTypes.INTEGER).get();
        int defaultCapacity = IntObjectHashMap.DEFAULT_CAPACITY;
        float defaultLoadFactor = IntObjectHashMap.DEFAULT_LOAD_FACTOR;
        int threshold = (int) (defaultCapacity * defaultLoadFactor);
        int prevCapacity = resizeAwareMap.currentCapacity();
        for (int i = 0; i < threshold ; i++) {
            resizeAwareMap.put(i, i);
        }

        // Get prediction before put
        long predictedBytes = resizeAwareMap.expectedCapacityIncreaseBytes();
        resizeAwareMap.put(threshold, threshold); // trigger resize
        int newCapacity = resizeAwareMap.currentCapacity();

        long deltaInBytes = (long) (newCapacity - prevCapacity) * (Integer.BYTES + RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        assertThat(predictedBytes).isEqualTo(deltaInBytes);
    }

    @Test
    public void test_put_initial_capacity_power_of_two() {
        Map<Integer, Integer> delegate = new HashMap<>();
        int capacity = 4;
        float loadFactor = 0.75f;
        int threshold = (int) (capacity * loadFactor);
        ResizeAwareMap<Integer, Integer> map = new ResizeAwareMap<>(delegate, capacity, loadFactor, false, RamUsageEstimator.NUM_BYTES_OBJECT_REF);

        for (int i = 0; i < threshold; i++) {
            assertThat(map.expectedCapacityIncrease()).isEqualTo(0);
            map.put(i, i);
        }
        assertThat(map.expectedCapacityIncrease()).isEqualTo(capacity);
    }

    @Test
    public void test_put_initial_capacity_is_rounded_up_to_the_next_power_of_two() {
        Map<Integer, Integer> delegate = new HashMap<>();
        ResizeAwareMap<Integer, Integer> map = new ResizeAwareMap<>(delegate, 5, 0.75f, false, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        // 5 is rounded up to 8, hence threshold is 8*0.75 = 6
        for (int i = 0; i < 6; i++) {
            assertThat(map.expectedCapacityIncrease()).isEqualTo(0);
            map.put(i, i);
        }
        assertThat(map.expectedCapacityIncrease()).isEqualTo(8);
    }

    @Test
    public void test_expectedCapacityIncrease_does_not_mutate_state() {
        Map<Integer, Integer> delegate = new HashMap<>();
        int capacity = 4;
        float loadFactor = 0.75f;
        int threshold = (int) (capacity * loadFactor);
        ResizeAwareMap<Integer, Integer> map = new ResizeAwareMap<>(delegate, capacity, loadFactor, false, RamUsageEstimator.NUM_BYTES_OBJECT_REF);

        for (int i = 0; i < threshold + 1; i++) {
            map.expectedCapacityIncrease();
        }
        // We did threshold + 1 calls, but actual capacity didn't change without put calls.
        assertThat(map.currentCapacity()).isEqualTo(capacity);
    }

    @Test
    public void test_capacity_growth_stops_at_maximum_capacity() {
        int maximumCapacity = 1 << 30;
        @SuppressWarnings("unchecked")
        Map<Integer, Integer> delegate = mock(Map.class);
        when(delegate.size()).thenReturn(Integer.MAX_VALUE - 1);

        ResizeAwareMap<Integer, Integer> map = new ResizeAwareMap<>(delegate, maximumCapacity, 0.75f, false, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        assertThat(map.expectedCapacityIncrease()).isEqualTo(0);
    }

    @Test
    public void test_put_with_duplicate_key_does_no_resize() {
        Map<Integer, Integer> delegate = new HashMap<>();
        int capacity = 4;
        float loadFactor = 0.75f;
        int threshold = (int) (capacity * loadFactor);
        ResizeAwareMap<Integer, Integer> map = new ResizeAwareMap<>(delegate, capacity, loadFactor, false, RamUsageEstimator.NUM_BYTES_OBJECT_REF);

        // Bringing to the point where it's about to resize.
        for (int i = 0; i < threshold; i++) {
            map.put(i, i);
        }
        assertThat(map.size()).isEqualTo(threshold);
        assertThat(map.currentCapacity()).isEqualTo(capacity);

        // Next put() calls with duplicate keys won't trigger resize.
        for (int i = 0; i < 3; i++) {
            map.put(i, i);
            assertThat(map.currentCapacity()).isEqualTo(capacity);
        }
    }
}
