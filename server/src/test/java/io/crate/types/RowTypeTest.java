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

import static io.crate.testing.Asserts.assertThat;

import java.util.List;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.data.RowN;

public class RowTypeTest extends ESTestCase {

    @Test
    public void test_row_type_streaming_roundtrip() throws Exception {
        var rowType = new RowType(List.of(DataTypes.STRING, DataTypes.INTEGER));

        var out = new BytesStreamOutput();
        rowType.writeTo(out);

        var in = out.bytes().streamInput();
        var streamedRowType = new RowType(in);

        assertThat(streamedRowType).isEqualTo(rowType);
    }

    @Test
    public void test_row_value_streaming_roundtrip() throws Exception {
        var row = new RowN(10, "foo", 23.4);
        var rowType = new RowType(List.of(DataTypes.INTEGER, DataTypes.STRING, DataTypes.DOUBLE));

        var out = new BytesStreamOutput();
        rowType.streamer().writeValueTo(out, row);

        var in = out.bytes().streamInput();
        var streamedRow = rowType.streamer().readValueFrom(in);

        assertThat(streamedRow).isEqualTo(row);
    }

    @Test
    public void test_row_type_create_type_from_signature_round_trip() {
        var expected = new RowType(
            List.of(DataTypes.INTEGER, DataTypes.STRING),
            List.of("field1", "field2"));

        var actual = expected.getTypeSignature().createType();

        assertThat(actual).isExactlyInstanceOf(RowType.class);
        assertThat(actual.getTypeParameters()).isEqualTo(expected.getTypeParameters());
    }
}
