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

import org.assertj.core.api.Assertions;
import org.junit.Test;

public class DateTypeTest {

    @Test
    public void testCastFromInvalidString() {
        Assertions.assertThatThrownBy(() -> DateType.INSTANCE.implicitCast("not-a-number"))
            .isExactlyInstanceOf(ClassCastException.class)
            .hasMessageContaining("Can't cast 'not-a-number' to date");
    }

    @Test
    public void testCastString() {
        assertThat(DateType.INSTANCE.implicitCast("86500000")).isEqualTo(86400000L);
    }

    @Test
    public void testCastDateString() {
        assertThat(DateType.INSTANCE.implicitCast("2020-02-09")).isEqualTo(1581206400000L);
        assertThat(DateType.INSTANCE.implicitCast("2020-02-09T17:50:44")).isEqualTo(1581206400000L);
    }

    @Test
    public void testCastFloatValue() {
        assertThat(DateType.INSTANCE.implicitCast(1422294644.581f)).isEqualTo(1422230400000L);
    }

    @Test
    public void testCastNumericNonFloatValue() {
        assertThat(DateType.INSTANCE.implicitCast(123)).isEqualTo(0L);
        assertThat(DateType.INSTANCE.implicitCast(86500000)).isEqualTo(86400000L);
        assertThat(DateType.INSTANCE.implicitCast(1422294644581L)).isEqualTo(1422230400000L);
    }

    @Test
    public void testCastNull() {
        assertThat(DateType.INSTANCE.implicitCast(null)).isNull();
    }
}
