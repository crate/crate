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

import org.junit.Test;

import io.crate.testing.PlainRamAccounting;

public class AccountableListTest {

    @Test
    public void test_list_accounts_for_shallow_size() throws Exception {
        PlainRamAccounting accounting = new PlainRamAccounting();
        AccountableList<Integer> list = new AccountableList<>(accounting::addBytes);
        assertThat(accounting.totalBytes()).isEqualTo(4); // Size

        int length = 100;
        for (int i = 0; i < length; i++) {
            list.add(i);
        }
        assertThat(accounting.totalBytes()).isEqualTo(468L);

        List<Integer> subList = list.subList(10, 20);
        assertThat(accounting.totalBytes()).isEqualTo(480L); // Sub list structures (pointer, offset and size).

        // Account for temporal storage overhead on list sorting.
        list.sort(Comparator.comparingInt(x -> x));
        assertThat(accounting.totalBytes()).isEqualTo(680L);

        // Account for temporal storage overhead on sub-list sorting.
        subList.sort(Comparator.comparingInt(x -> x));
        assertThat(accounting.totalBytes()).isEqualTo(704L);
    }
}
