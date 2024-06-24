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

import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeFalse;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class IpTypeTest extends DataTypeTestCase<String> {

    @Override
    public DataType<String> getType() {
        return IpType.INSTANCE;
    }

    @Test
    public void test_sanitize_value() {
        assertThat(IpType.INSTANCE.sanitizeValue(null)).isNull();
        assertThat(IpType.INSTANCE.sanitizeValue("127.0.0.1")).isEqualTo("127.0.0.1");
    }

    @Test
    public void test_sanitize_invalid_ip_value_throws_exception() {
        assertThatThrownBy(() -> IpType.INSTANCE.sanitizeValue("2000.0.0.1"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Failed to validate ip [2000.0.0.1], not a valid ipv4 address");
    }

    @Test
    public void test_cast_negative_bigint_value_to_ip_throws_exception() {
        assertThatThrownBy(() -> IpType.INSTANCE.implicitCast(Long.MIN_VALUE))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Failed to convert long value: -9223372036854775808 to ipv4 address");
    }

    @Override
    public void test_reference_resolver_docvalues_off() throws Exception {
        assumeFalse("IpType cannot disable column store", true);
    }

    @Override
    public void test_reference_resolver_index_and_docvalues_off() throws Exception {
        assumeFalse("IpType cannot disable column store", true);
    }
}
