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
import static io.crate.types.DataTypes.REGTYPE;
import org.assertj.core.api.Assertions;

import org.junit.Test;

public class RegtypeTypeTest extends DataTypeTestCase<Regtype> {
    @Override
    protected DataDef<Regtype> getDataDef() {
        return DataDef.fromType(RegtypeType.INSTANCE);
    }

    @Test
    public void test_implicit_cast_integer_to_regtype() {
        Regtype regtype = REGTYPE.implicitCast(16);
        assertThat(regtype.oid()).isEqualTo(16);
        assertThat(regtype.name()).isEqualTo("bool");
    }

    @Test
    public void test_implicit_cast_integer_that_does_not_match_a_type() {
        // Mimics PostgreSQL behavior.
        // PostgreSQL passes the int through if it is not a data type oid.
        // For example, `select 123456789::regtype;` returns 123456789, and
        // does not raise an error
        Regtype regtype = REGTYPE.implicitCast(123456789);
        assertThat(regtype.oid()).isEqualTo(123456789);
        assertThat(regtype.name()).isEqualTo("123456789");
    }

    @Test
    public void test_implicit_cast_text_to_regtype() {
        Regtype regtype = REGTYPE.implicitCast("bool");
        assertThat(regtype.oid()).isEqualTo(16);
        assertThat(regtype.name()).isEqualTo("bool");
    }

    @Test
    public void test_cannot_cast_long_outside_int_range_to_regtype() {
        Assertions.assertThatThrownBy(() -> REGTYPE.implicitCast(Integer.MAX_VALUE + 42L))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2147483689 is outside of `int` range and cannot be cast to the regtype type");
    }

    @Test
    public void test_implicit_cast_long_to_regtype() {
        Regtype regtype = REGTYPE.implicitCast(16L);
        assertThat(regtype.oid()).isEqualTo(16);
        assertThat(regtype.name()).isEqualTo("bool");
    }

    @Test
    public void test_implicit_cast_regtype_to_regtype() {
        Regtype original = new Regtype("bool");
        Regtype regtype = REGTYPE.implicitCast(original);
        assertThat(regtype).isSameAs(original);
    }

    @Test
    public void test_raises_if_implicit_cast_not_supported_for_object() {
        Assertions.assertThatThrownBy(() -> REGTYPE.implicitCast(true))
                .isExactlyInstanceOf(ClassCastException.class)
                .hasMessageContaining("Can't cast ");
    }

    @Test
    public void test_raises_if_cannot_find_data_type() {
        // Mimics PostgreSQL behavior.
        // PostgreSQL raises an error when running a query like this: `select
        // 'not_existing'::regtype;`
        Assertions.assertThatThrownBy(() -> REGTYPE.implicitCast("not_existing"))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type not_existing does not exist");
    }

}
