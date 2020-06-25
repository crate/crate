/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.types;

import io.crate.test.integration.CrateUnitTest;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class DoubleTypeTest extends CrateUnitTest {

    @Test
    public void test_cast_text_to_double() {
        assertThat(DoubleType.INSTANCE.implicitCast("123"), is(123d));
    }

    @Test
    public void test_cast_long_to_double() {
        assertThat(DoubleType.INSTANCE.implicitCast(123L), is(123d));
    }

    @Test
    public void text_cast_object_to_double_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast '{}' to double precision");
        DoubleType.INSTANCE.implicitCast(Map.of());
    }

    @Test
    public void text_cast_boolean_to_double_throws_exception() {
        expectedException.expect(ClassCastException.class);
        expectedException.expectMessage("Can't cast 'true' to double precision");
        DoubleType.INSTANCE.implicitCast(true);
    }
}
