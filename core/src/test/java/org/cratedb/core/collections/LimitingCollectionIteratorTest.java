/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.core.collections;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LimitingCollectionIteratorTest {


    @Test
    public void testLimitingCollectionIterator() {

        List<Integer> rows = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            rows.add(i);
        }

        List<Integer> newRows = new ArrayList<>();
        LimitingCollectionIterator<Integer> integers = new LimitingCollectionIterator<>(rows, 2);

        for (Integer integer : integers) {
            newRows.add(integer);
        }

        assertEquals(2, newRows.size());
    }

    @Test
    public void testLimitingCollectionIteratorIsEmptyLimitZero() {

        List<Integer> rows = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            rows.add(i);
        }

        LimitingCollectionIterator<Integer> integers = new LimitingCollectionIterator<>(rows, 0);
        assertThat(integers.isEmpty(), is(true));
    }

    @Test
    public void testLimitingCollectionIteratorIsEmptyEmptyInput() {

        List<Integer> rows = new ArrayList<>(0);

        LimitingCollectionIterator<Integer> integers = new LimitingCollectionIterator<>(rows, 10);
        assertThat(integers.isEmpty(), is(true));
    }
}
