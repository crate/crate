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

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class IpTypeTest extends ESTestCase {

    @Test
    public void test_sanitize_value() {
        assertThat(IpType.INSTANCE.sanitizeValue(null), is(nullValue()));
        assertThat(IpType.INSTANCE.sanitizeValue("127.0.0.1"), is("127.0.0.1"));
    }

    @Test
    public void test_sanitize_invalid_ip_value_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Failed to validate ip [2000.0.0.1], not a valid ipv4 address");
        IpType.INSTANCE.sanitizeValue("2000.0.0.1");
    }

    @Test
    public void test_cast_negative_bigint_value_to_ip_throws_exception() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Failed to convert long value: -9223372036854775808 to ipv4 address");
        IpType.INSTANCE.implicitCast(Long.MIN_VALUE);
    }
}
