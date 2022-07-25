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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.BitSet;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;

@RunWith(JUnitQuickcheck.class)
public class BitStringTest {

    public static class BitStringGen extends Generator<BitString> {

        public BitStringGen() {
            super(BitString.class);
        }

        @Override
        public BitString generate(SourceOfRandomness random, GenerationStatus status) {
            int length = random.nextInt(3, 6);
            BitSet bitSet = new BitSet(length);
            for (int i = 0; i < length; i++) {
                bitSet.set(i, random.nextBoolean());
            }
            return new BitString(bitSet, length);
        }
    }

    @Test
    public void test_can_parse_bit_string_with_zeros_and_ones() {
        BitString bit = BitString.ofRawBits("00000110");
        BitSet expected = new BitSet(8);
        expected.set(5, true);
        expected.set(6, true);
        assertThat(bit.bitSet()).isEqualTo((expected));
    }

    @Test
    public void test_bit_string_cannot_contain_values_other_than_zeros_or_ones() {
        assertThatThrownBy(
            () -> BitString.ofRawBits("0021💀"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Bit string must only contain `0` or `1` values. Encountered: 2");
    }

    @Test
    public void test_can_render_bitstring_as_string() {
        String text = "00000110";
        BitString bit = BitString.ofRawBits(text);
        assertThat(bit.asPrefixedBitString()).isEqualTo("B'00000110'");
    }

    @Test
    public void test_lexicographically_order() {
        assertThat(BitString.ofRawBits("1001").compareTo(BitString.ofRawBits("1111"))).isEqualTo(-1);
        assertThat(BitString.ofRawBits("1111").compareTo(BitString.ofRawBits("1001"))).isEqualTo(1);
        assertThat(BitString.ofRawBits("111").compareTo(BitString.ofRawBits("0001")))
            .isEqualTo("111".compareTo("0001"));
    }


    @Property
    public void test_bitstring_compare_behaves_like_asBitString_compareTo(@From(BitStringGen.class) BitString a,
                                                                          @From(BitStringGen.class) BitString b) {
        assertThat(a.compareTo(b)).isEqualTo(Integer.signum(a.asPrefixedBitString().compareTo(b.asPrefixedBitString())));
    }
}
