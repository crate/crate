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

import java.util.BitSet;
import java.util.Map;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.sql.tree.BitString;
import io.crate.testing.DataTypeTesting;

public class UndefinedTypeTest extends ESTestCase {

    private void testRoundTrip(DataType<?> type) throws Exception {
        var valueOut = DataTypeTesting.getDataGenerator(type).get();
        var out = new BytesStreamOutput();

        DataTypes.UNDEFINED.writeValueTo(out, valueOut);
        var in = out.bytes().streamInput();

        var valueIn = DataTypes.UNDEFINED.readValueFrom(in);
        assertThat(valueIn).isEqualTo(valueOut);
    }

    @Test
    public void test_undefined_type_can_stream_big_decimal_values() throws Exception {
        testRoundTrip(DataTypes.NUMERIC);
    }

    @Test
    public void test_undefined_type_can_stream_timetz_values() throws Exception {
        testRoundTrip(DataTypes.INTERVAL);
    }

    @Test
    public void test_undefined_type_can_stream_interval_values() throws Exception {
        testRoundTrip(DataTypes.INTERVAL);
    }

    @Test
    public void test_undefined_type_can_stream_geo_point_values() throws Exception {
        testRoundTrip(DataTypes.GEO_POINT);
    }

    @Test
    public void test_can_stream_bitstring_values() throws Exception {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(1);
        BitString bitString = new BitString(bits, 4);
        var map = Map.of("foo", bitString);

        var out = new BytesStreamOutput();
        DataTypes.UNDEFINED.writeValueTo(out, map);

        var in = out.bytes().streamInput();
        var mapIn = DataTypes.UNDEFINED.readValueFrom(in);
        assertThat(map).isEqualTo(mapIn);
    }
}
