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

package io.crate.collections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class TwoLevelHashMapTest {

    @Test
    public void test_put_and_get_roundtrip_across_many_buckets() throws Exception {
        var map = new TwoLevelHashMap<Integer, String>();
        for (int i = 0; i < 10_000; i++) {
            map.put(i, "v" + i);
        }
        assertThat(map.size()).isEqualTo(10_000);
        for (int i = 0; i < 10_000; i++) {
            assertThat(map.get(i)).isEqualTo("v" + i);
        }
    }

    @Test
    public void test_put_overwrites_existing_key_and_returns_previous_value() throws Exception {
        var map = new TwoLevelHashMap<String, Integer>();
        assertThat(map.put("a", 1)).isNull();
        assertThat(map.put("a", 2)).isEqualTo(1);
        assertThat(map.get("a")).isEqualTo(2);
        assertThat(map.size()).isEqualTo(1);
    }

    @Test
    public void test_get_and_contains_key_on_missing_key() throws Exception {
        var map = new TwoLevelHashMap<String, Integer>();
        map.put("a", 1);
        assertThat(map.get("missing")).isNull();
        assertThat(map.containsKey("missing")).isFalse();
        assertThat(map.containsKey("a")).isTrue();
    }

    @Test
    public void test_remove() throws Exception {
        var map = new TwoLevelHashMap<String, Integer>();
        map.put("a", 1);
        assertThat(map.remove("a")).isEqualTo(1);
        assertThat(map.containsKey("a")).isFalse();
        assertThat(map.size()).isEqualTo(0);
        assertThat(map.isEmpty()).isTrue();
    }

    @Test
    public void test_clear() throws Exception {
        var map = new TwoLevelHashMap<Integer, Integer>();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        map.clear();
        assertThat(map.isEmpty()).isTrue();
        assertThat(map.size()).isEqualTo(0);
    }

    @Test
    public void test_iterator_visits_every_entry_exactly_once() throws Exception {
        var map = new TwoLevelHashMap<Integer, String>();
        Map<Integer, String> expected = new HashMap<>();
        for (int i = 0; i < 1_000; i++) {
            map.put(i, "v" + i);
            expected.put(i, "v" + i);
        }

        Set<Integer> seenKeys = new HashSet<>();
        int count = 0;
        for (Map.Entry<Integer, String> entry : map) {
            assertThat(seenKeys.add(entry.getKey())).isTrue();
            assertThat(entry.getValue()).isEqualTo(expected.get(entry.getKey()));
            count++;
        }
        assertThat(count).isEqualTo(expected.size());
    }

    @Test
    public void test_empty_map_iterator_has_no_elements() throws Exception {
        var map = new TwoLevelHashMap<Integer, String>();
        assertThat(map.iterator().hasNext()).isFalse();
    }

    @Test
    public void test_custom_bits_for_bucket_controls_bucket_count() throws Exception {
        var map = new TwoLevelHashMap<Integer, Integer>(4);
        assertThat(map.numBuckets()).isEqualTo(16);
    }

    @Test
    public void test_entries_are_distributed_across_multiple_buckets() throws Exception {
        var map = new TwoLevelHashMap<Integer, Integer>();
        for (int i = 0; i < 10_000; i++) {
            map.put(i, i);
        }

        long nonEmptyBuckets = 0;
        for (int i = 0; i < map.numBuckets(); i++) {
            if (!map.bucket(i).isEmpty()) {
                nonEmptyBuckets++;
            }
        }
        assertThat(nonEmptyBuckets).isGreaterThan(1);
    }
}
