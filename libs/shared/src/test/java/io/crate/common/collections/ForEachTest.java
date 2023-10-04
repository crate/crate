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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;


public class ForEachTest {

    private void assertForEachCalledOnAllElements(Object elements, int expected) {
        final AtomicInteger sum = new AtomicInteger(0);
        ForEach.forEach(elements, input -> sum.getAndAdd((int) input));
        assertThat(sum.get()).isEqualTo(expected);
    }

    @Test
    public void testPrimitiveArray() throws Exception {
        int[] array = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        assertForEachCalledOnAllElements(array, 55);
    }

    @Test
    public void testEmptyPrimitiveArray() throws Exception {
        int[] array = new int[0];
        assertForEachCalledOnAllElements(array, 0);
    }

    @Test
    public void testArray() throws Exception {
        Integer[] array = new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        assertForEachCalledOnAllElements(array, 55);
    }

    @Test
    public void testCollection() throws Exception {
        Set<Integer> set = Set.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertForEachCalledOnAllElements(set, 55);
    }
}
