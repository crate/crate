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
import java.util.List;

import org.junit.jupiter.api.Test;


public class CartesianListTest {

    @SafeVarargs
    static <T> List<List<T>> cartesianList(List<T> ... list) {
        return CartesianList.of(Arrays.asList(list));
    }

    @Test
    public void testCartesianProduct_zeroary() {
        var result = cartesianList(List.of());
        assertThat(result).isEqualTo((List.of()));
    }

    @Test
    public void testCartesianProduct_unary() {
        var result = cartesianList(List.of(1, 2));
        assertThat(result).containsExactly(List.of(1), List.of(2));
    }

    @Test
    public void testCartesianProduct_binary0x0() {
        var result = cartesianList(List.of(), List.of());
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    public void testCartesianProduct_binary0x1() {
        var result = cartesianList(List.of(), List.of(1));
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    public void testCartesianProduct_binary1x0() {
        var result = cartesianList(List.of(1), List.of());
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    public void testCartesianProduct_binary1x1() {
        assertThat(cartesianList(List.of(1), List.of(2))).containsExactly(List.of(1, 2));
    }

    @Test
    public void testCartesianProduct_binary1x2() {
        assertThat(cartesianList(List.of(1), List.of(2, 3))).containsExactly(List.of(1, 2), List.of(1, 3));
    }

    @Test
    public void testCartesianProduct_binary2x2() {
        assertThat(cartesianList(List.of(1, 2), List.of(3, 4))).containsExactly(
            List.of(1, 3),
            List.of(1, 4),
            List.of(2, 3),
            List.of(2, 4));
    }

    @Test
    public void testCartesianProduct_2x2x2() {
        assertThat(cartesianList(List.of(0, 1), List.of(0, 1), List.of(0, 1))).containsExactly(
            List.of(0, 0, 0),
            List.of(0, 0, 1),
            List.of(0, 1, 0),
            List.of(0, 1, 1),
            List.of(1, 0, 0),
            List.of(1, 0, 1),
            List.of(1, 1, 0),
            List.of(1, 1, 1));
    }

    @Test
    public void testCartesianProduct_contains() {
        List<List<Integer>> actual = cartesianList(List.of(1, 2), List.of(3, 4));
        assertThat(actual).containsExactly(
            List.of(1, 3),
            List.of(1, 4),
            List.of(2, 3),
            List.of(2, 4));
    }
}
