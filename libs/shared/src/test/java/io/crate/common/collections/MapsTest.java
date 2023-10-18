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

package io.crate.common.collections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class MapsTest {

    @Test
    void testAccessByPath() {
        Map<String, Object> map = new HashMap<>();
        assertThat(Maps.getByPath(map, "")).isNull();
        assertThat(Maps.getByPath(map, "a.b.c")).isNull();

        map.put("a", "b");

        assertThat(Maps.getByPath(map, "a")).isEqualTo("b");
        assertThat(Maps.getByPath(map, "a.b")).isNull();

        Map<String, Object> nestedMap = Map.of("b", 123);
        map.put("a", nestedMap);
        assertThat(Maps.getByPath(map, "a")).isEqualTo(nestedMap);
        assertThat(Maps.getByPath(map, "a.b")).isEqualTo(123);
        assertThat(Maps.getByPath(map, "a.b.c")).isNull();
        assertThat(Maps.getByPath(map, "a.c")).isNull();
        assertThat(Maps.getByPath(map, "b.c")).isNull();
    }

    @Test
    void testMergeIntoSourceWithNullEntry() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("o", null);

        Maps.mergeInto(m, "o", Collections.singletonList("x"), 10);
        assertThat(m).isEqualTo(Map.of("o", Map.of("x", 10)));
    }

    @Test
    void testMergeNestedIntoWithNullEntry() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("o", null);

        Maps.mergeInto(m, "o", Arrays.asList("x", "y"), 10);
        assertThat(m).isEqualTo(Map.of("o", Map.of("x", Map.of("y", 10))));
    }

    @Test
    void test_merge_maps() {
        Map<Integer, Set<Integer>> m1 = Map.of(1, Set.of(1));
        Map<Integer, Set<Integer>> m2 = Map.of(1, Set.of(2), 2, Set.of(3));

        assertThat(Maps.merge(m1, m2, Sets::union)).isEqualTo(Map.of(1, Set.of(1, 2), 2, Set.of(3)));

        m1 = Map.of(1, Set.of(0));
        m2 = Map.of(1, Set.of(), 2, Set.of(0));

        assertThat(Maps.merge(m1, m2, Sets::union)).isEqualTo(Map.of(1, Set.of(0), 2, Set.of(0)));

    }
}
