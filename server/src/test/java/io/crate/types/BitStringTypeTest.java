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

package io.crate.types;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.Test;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.settings.SessionSettings;
import io.crate.sql.tree.BitString;
import io.crate.testing.DataTypeTesting;

public class BitStringTypeTest extends DataTypeTestCase<BitString> {

    @Override
    public DataType<BitString> getType() {
        return BitStringType.INSTANCE_ONE;
    }

    private static final SessionSettings SESSION_SETTINGS = CoordinatorTxnCtx.systemTransactionContext().sessionSettings();

    @Test
    public void test_value_streaming_roundtrip() throws Exception {
        BitStringType type = new BitStringType(randomInt(80));
        Supplier<BitString> dataGenerator = DataTypeTesting.getDataGenerator(type);
        var value = dataGenerator.get();

        var out = new BytesStreamOutput();
        type.writeValueTo(out, value);
        StreamInput in = out.bytes().streamInput();
        assertThat(type.readValueFrom(in)).isEqualTo(value);
    }

    @Test
    public void test_value_for_insert_only_allows_exact_length_matches() throws Exception {
        BitStringType type = new BitStringType(3);
        Assertions.assertThatThrownBy(() -> type.valueForInsert(BitString.ofRawBits("00010001")))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("bit string length 8 does not match type bit(3)");
    }

    @Test
    public void test_explicit_cast_can_trim_bitstring() throws Exception {
        BitStringType type = new BitStringType(3);
        BitString result = type.explicitCast(BitString.ofRawBits("1111"), SESSION_SETTINGS);
        assertThat(result).isEqualTo(BitString.ofRawBits("111"));
    }

    @Test
    public void test_explicit_cast_can_extend_bitstring() throws Exception {
        BitStringType type = new BitStringType(4);
        BitString result = type.explicitCast(BitString.ofRawBits("111"), SESSION_SETTINGS);
        assertThat(result).isEqualTo(BitString.ofRawBits("1110"));
    }
}
