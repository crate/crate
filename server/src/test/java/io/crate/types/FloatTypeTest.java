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

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;


import static org.hamcrest.Matchers.is;

public class FloatTypeTest extends ESTestCase {


    @Test
    public void test_cast_long_to_real() {
        assertThat(FloatType.INSTANCE.value(123L), is(123.0f));
    }

    @Test
    public void test_cast_text_to_real() {
        assertThat(FloatType.INSTANCE.value("123"), is(123.0f));
    }

    @Test
    public void test_sanitize_numeric_value() {
        assertThat(FloatType.INSTANCE.value(1d), is(1f));
    }

    @Test
    public void test_negative_max_float_value_can_be_casted_to_float() throws Exception {
        assertThat(FloatType.INSTANCE.value(-3.4028235e38d), is(- Float.MAX_VALUE));
        assertThat(FloatType.INSTANCE.value(-3.4028235e38f), is(- Float.MAX_VALUE));
    }
}
