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
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */


package io.crate.sql.tree;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.BitSet;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class BitStringTest {

    @Test
    public void test_can_parse_bit_string_with_zeros_and_ones() {
        BitString bit = BitString.of("00000110");
        BitSet expected = new BitSet(8);
        expected.set(5, true);
        expected.set(6, true);
        assertThat(bit.bitSet(), is(expected));
    }

    @Test
    public void test_bit_string_cannot_contain_values_other_than_zeros_or_ones() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> BitString.of("0021💀"));
    }

    @Test
    public void test_can_render_bistring_as_string() {
        String text = "00000110";
        BitString bit = BitString.of(text);
        assertThat(bit.asBitString(), is("B'00000110'"));
    }
}
