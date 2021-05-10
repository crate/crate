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

import static io.crate.types.DataTypes.REGPROC;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.Set;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.metadata.pgcatalog.OidHash;

public class RegprocTypeTest extends ESTestCase {

    @Test
    public void test_implicit_cast_regproc_to_integer() {
        Regproc proc = Regproc.of("func");
        assertThat(
            DataTypes.INTEGER.implicitCast(proc),
            is(OidHash.functionOid(proc.asDummySignature())));
    }

    @Test
    public void test_implicit_cast_integer_to_regproc() {
        assertThat(REGPROC.implicitCast(1), is(Regproc.of(1, "1")));
    }

    @Test
    public void test_implicit_cast_text_to_regproc() {
        assertThat(
            REGPROC.implicitCast("func"),
            is(Regproc.of("func")));
    }

    @Test
    public void test_implicit_cast_regproc_to_not_allowed_type_throws_class_cast_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '1.1' to regproc");
        REGPROC.implicitCast(1.1);
    }

    @Test
    public void test_convertible_only_to_text_and_integer_types() {
        assertThat(
            DataTypes.ALLOWED_CONVERSIONS.get(RegprocType.ID),
            is(Set.of(StringType.ID, IntegerType.ID))
        );
    }

    @Test
    public void test_insert_for_values_throws_not_supported_exception() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            REGPROC.getName() + " cannot be used in insert statements");
        REGPROC.valueForInsert(Regproc.of("func"));
    }

    @Test
    public void test_regproc_value_streaming() throws IOException {
        Regproc expected = Regproc.of(
            random().nextInt(),
            String.valueOf(random().nextInt()));

        BytesStreamOutput out = new BytesStreamOutput();
        REGPROC.writeValueTo(out, expected);

        StreamInput in = out.bytes().streamInput();
        Regproc actual = REGPROC.readValueFrom(in);

        assertThat(expected, is(actual));
    }

    @Test
    public void test_type_serialization_round_trip() throws IOException {
        var out = new BytesStreamOutput();
        DataTypes.toStream(REGPROC, out);

        var in = out.bytes().streamInput();
        assertThat(DataTypes.fromStream(in), is(REGPROC));
    }
}
