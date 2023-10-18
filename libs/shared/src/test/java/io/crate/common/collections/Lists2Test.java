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

import static io.crate.common.collections.Lists2.findFirstGTEProbeValue;
import static io.crate.common.collections.Lists2.findFirstLTEProbeValue;
import static io.crate.common.collections.Lists2.findFirstNonPeer;
import static io.crate.common.collections.Lists2.findFirstPreviousPeer;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class Lists2Test {

    private Comparator<Integer> integerComparator;

    @BeforeEach
    public void setupComparator() {
        integerComparator = Comparator.comparingInt(x -> x);
    }

    @Test
    void testConcatReturnsANewListWithOneItemAdded() {
        assertThat(Lists2.concat(Arrays.asList(1, 2), 3)).containsExactly(1, 2, 3);
    }

    @Test
    void testFindFirstWithAllUnique() {
        var numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8);
        assertThat(findFirstNonPeer(numbers, 0, numbers.size() - 1, integerComparator)).isEqualTo(1);
    }

    @Test
    void testFindFirstNonPeerAllSame() {
        var numbers = List.of(1, 1, 1, 1, 1, 1, 1, 1);
        assertThat(findFirstNonPeer(numbers, 0, numbers.size() - 1, integerComparator)).isEqualTo(numbers.size() - 1);
    }

    @Test
    void testFindFirstPreviousPeerReturnZeroForNullComparator() {
        var numbers = List.of(1, 1, 2, 4, 4, 4, 4, 5);
        assertThat(findFirstPreviousPeer(numbers, 5, null)).isEqualTo(0);
    }

    @Test
    void testFindFirstPreviousPeerForFirstElementReturnsZero() {
        var numbers = List.of(1, 1, 2, 4, 4, 4, 4, 5);
        assertThat(findFirstPreviousPeer(numbers, 0, integerComparator)).isEqualTo(0);
    }

    @Test
    void testFindFirstPreviousPeerReturnsFirstOccuranceOfPeer() {
        var numbers = List.of(1, 1, 2, 4, 4, 4, 4, 5);
        assertThat(findFirstPreviousPeer(numbers, 5, integerComparator)).isEqualTo(3);
    }

    @Test
    void testFindFirstPreviousPeerReturnsItemIndexIfThereAreNoPeers() {
        var numbers = List.of(1, 2, 3, 4, 5, 6);
        for (int i = 0; i < numbers.size(); i++) {
            assertThat(findFirstPreviousPeer(numbers, i, integerComparator)).isEqualTo(i);
        }
    }

    @Test
    void test_find_first_gte_probe_when_exists_in_slice() {
        var numbers = List.of(1, 2, 3, 6, 7, 8);
        assertThat(findFirstGTEProbeValue(numbers, 0, 4, 4, integerComparator)).isEqualTo(3);
    }

    @Test
    void test_find_first_gte_probe_when_greater_than_all_items_is_minus_one() {
        var numbers = List.of(1, 2, 3, 6, 7, 8);
        assertThat(findFirstGTEProbeValue(numbers, 0, 3, 4, integerComparator)).isEqualTo(-1);
    }

    @Test
    void test_find_first_lte_probe_when_exists_in_slice() {
        var numbers = List.of(1, 2, 3, 6, 7, 8);
        assertThat(findFirstLTEProbeValue(numbers, numbers.size(), 3, 7, integerComparator)).isEqualTo(4);
    }

    @Test
    void test_find_first_lte_probe_when_less_than_all_items_is_minus_one() {
        var numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8);
        assertThat(findFirstLTEProbeValue(numbers, numbers.size(), 5, 0, integerComparator)).isEqualTo(-1);
    }

    @Test
    void test_collections_are_equal() {
        var list = List.of(1, 2, 3);
        var set = new LinkedHashSet<>(List.of(1, 2, 3));
        assertThat(Lists2.equals(list, set)).isTrue();
    }

    @Test
    void test_collections_different_order_arent_equal() {
        var list = List.of(1, 2, 3);
        var set = new LinkedHashSet<>(List.of(1, 3, 2));
        assertThat(Lists2.equals(list, set)).isFalse();
    }

    @Test
    void test_collections_different_size_arent_equal() {
        var list = List.of(1, 2, 3);
        var set = new LinkedHashSet<>(List.of(1, 2));
        assertThat(Lists2.equals(list, set)).isFalse();
    }
}
