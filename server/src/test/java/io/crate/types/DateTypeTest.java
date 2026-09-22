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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class DateTypeTest extends DataTypeTestCase<LocalDate> {

    @Override
    protected DataDef<LocalDate> getDataDef() {
        return DataDef.fromType(DataTypes.DATE);
    }

    @Test
    public void testCastFromInvalidString() {
        Assertions.assertThatThrownBy(() -> DateType.INSTANCE.implicitCast("not-a-number"))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessageContaining("Can't cast 'not-a-number' to date");
    }

    @Test
    public void testCastString() {
        assertThat(DateType.INSTANCE.implicitCast("86500000")).isEqualTo(LocalDate.of(1970, 1, 2));
    }

    @Test
    public void testCastDateString() {
        assertThat(DateType.INSTANCE.implicitCast("2020-02-09")).isEqualTo(LocalDate.of(2020, 2, 9));
        assertThat(DateType.INSTANCE.implicitCast("2020-02-09T17:50:44")).isEqualTo(LocalDate.of(2020, 2, 9));
    }

    @Test
    public void testCastFloatValue() {
        assertThat(DateType.INSTANCE.implicitCast(1422294644.581f)).isEqualTo(LocalDate.of(2015, 1, 26));
    }

    @Test
    public void testCastNumericNonFloatValue() {
        assertThat(DateType.INSTANCE.implicitCast(123)).isEqualTo(LocalDate.of(1970, 1, 1));
        assertThat(DateType.INSTANCE.implicitCast(86500000)).isEqualTo(LocalDate.of(1970, 1, 2));
        assertThat(DateType.INSTANCE.implicitCast(1422294644581L)).isEqualTo(LocalDate.of(2015, 1, 26));
    }

    @Test
    public void testCastNull() {
        assertThat(DateType.INSTANCE.implicitCast(null)).isNull();
    }

    @Test
    public void test_cannot_use_local_date_unsafe_for_timestamp_conversion() throws Exception {
        assertThatThrownBy(() -> DateType.INSTANCE.implicitCast(LocalDate.MAX))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("date (+999999999-12-31) exceeds allowed max value (+292278994-08-16)");

        assertThatThrownBy(() -> DateType.INSTANCE.implicitCast(LocalDate.MIN))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("date (-999999999-01-01) exceeds allowed min value (-292275055-05-18)");
    }
}
