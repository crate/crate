/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.common.collections;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static io.crate.common.collections.Lists2.arePeers;
import static io.crate.common.collections.Lists2.findFirstNonPeer;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class Lists2Test {

    @Test
    public void testConcatReturnsANewListWithOneItemAdded() {
        assertThat(Lists2.concat(Arrays.asList(1, 2), 3), Matchers.contains(1, 2, 3));
    }

    @Test
    public void testFindFirstWithAllUnique() {
        var numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8);
        assertThat(
            findFirstNonPeer(numbers, 0, numbers.size() - 1, Comparator.comparingInt(x -> x)),
            is(1)
        );
    }
    @Test
    public void testFindFirstNonPeerAllSame() {
        var numbers = List.of(1, 1, 1, 1, 1, 1, 1, 1);
        assertThat(
            findFirstNonPeer(numbers, 0, numbers.size() - 1, Comparator.comparingInt(x -> x)),
            is(numbers.size() - 1)
        );
    }

    @Test
    public void testEqualValuesArePeers() {
        var numbers = List.of(1, 2, 2, 4, 5, 6, 6, 7);
        assertThat(arePeers(numbers, 1, 2, Comparator.comparing(x -> x)),
                   is(true));
        assertThat(arePeers(numbers, 5, 6, Comparator.comparing(x -> x)),
                   is(true));
    }

    @Test
    public void testAllItemsArePeersForNullComparator() {
        var numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8);
        for (int i = 0; i < numbers.size() - 1; i++) {
            assertThat(
                arePeers(numbers, i, i + 1, null),
                is(true)
            );
        }
    }

    @Test
    public void testArePeersWithAllUniqueReturnsFalse() {
        var numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8);
        for (int i = 0; i < numbers.size() - 1; i++) {
            assertThat(
                arePeers(numbers, i, i + 1, Comparator.comparingInt(x -> x)),
                is(false)
            );
        }
    }
}
