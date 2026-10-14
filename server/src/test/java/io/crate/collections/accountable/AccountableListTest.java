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

package io.crate.collections.accountable;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Test;

import io.crate.testing.PlainRamAccounting;

public class AccountableListTest {

    @Test
    public void test_list_accounts_for_shallow_size() throws Exception {
        PlainRamAccounting accounting = new PlainRamAccounting();
        AccountableList<Integer> list = new AccountableList<>(accounting::addBytes);
        assertThat(accounting.totalBytes()).isEqualTo(32); // Size

        int length = 100;
        for (int i = 0; i < length; i++) {
            list.add(i);
        }
        assertThat(accounting.totalBytes()).isEqualTo(488);

        List<Integer> subList = list.subList(10, 20);
        assertThat(accounting.totalBytes()).isEqualTo(500); // Sub list structures (pointer, offset and size).

        // Temporal storage overhead on list sorting is not accounted for.
        list.sort(Comparator.comparingInt(x -> x));
        assertThat(accounting.totalBytes()).isEqualTo(500);

        // Temporal storage overhead on sub-list sorting is not accounted for.
        subList.sort(Comparator.comparingInt(x -> x));
        assertThat(accounting.totalBytes()).isEqualTo(500);
    }

    @Test
    public void test_accounts_addAll() {
        // list1 adds 15 elements one by one.
        // list2 adds 15 elements in one go.
        // 15 was chosen because that capacity is reached when the list grows,
        // i.e. a list with the size of 15 also has the capacity of 15.
        // Both lists should have the same capacity and same accounted memory.
        PlainRamAccounting acct1 = new PlainRamAccounting();
        AccountableList<Integer> list1 = new AccountableList<>(acct1::addBytes);
        assertThat(acct1.totalBytes()).isEqualTo(32); // Size

        int length = 15;
        for (int i = 0; i < length; i++) {
            list1.add(i);
        }
        // growth:
        // initial: 32
        // 1st growth, to 10 elements, array header 16 bytes + 10 elements * 4 bytes, aligned to 56, total = 88
        // 2nd growth, to 15 elements, 16 + 15 * 4 = 76, aligned to 80, total = 112
        assertThat(acct1.totalBytes()).isEqualTo(112);

        // Add 15 elements at once. This makes the list's capacity grow to 15.
        // The resulting AccountableLists should be the same, and use the same amount of memory.
        PlainRamAccounting acct2 = new PlainRamAccounting();
        AccountableList<Integer> list2 = new AccountableList<>(acct2::addBytes);
        assertThat(acct2.totalBytes()).isEqualTo(32); // Size

        list2.addAll(IntStream.range(0, length).boxed().toList());
        assertThat(acct2.totalBytes()).isEqualTo(112);

        assertThat(list1.equals(list2));
    }
}
