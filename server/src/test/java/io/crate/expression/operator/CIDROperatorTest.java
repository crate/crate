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

package io.crate.expression.operator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.exceptions.ConversionException;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class CIDROperatorTest extends ScalarTestCase {

    @Test
    public void test_ipv4_operands_in_wrong_order() {
        assertThatThrownBy(() -> assertEvaluate("'192.168.0.1/24' << '192.168.0.1'", false))
            .isExactlyInstanceOf(ConversionException.class);
    }

    @Test
    public void test_ipv4_operands_in_wrong_order_from_query() {
        assertThatThrownBy(() -> assertEvaluate("name << ip",
                false,
                Literal.of("192.168.0.1/24"), Literal.of("192.168.0.1")))
            .isExactlyInstanceOf(ConversionException.class);
    }

    @Test
    public void test_ipv6_operands_in_wrong_order() {
        assertThatThrownBy(() -> assertEvaluate("'2001:db8::1/120' << '2001:db8:0:0:0:0:0:c8'::ip", false))
            .isExactlyInstanceOf(ConversionException.class);
    }

    @Test
    public void test_ipv6_operands_in_wrong_order_from_query() {
        assertThatThrownBy(() -> assertEvaluate("name << ip",
                false,
                Literal.of("2001:db8::1/120"), Literal.of("2001:db8:0:0:0:0:0:c8")))
            .isExactlyInstanceOf(ConversionException.class);
    }

    @Test
    public void test_ipv4_both_operands_are_ips() {
        assertThatThrownBy(() -> assertEvaluate("'192.168.0.1'::ip << '192.168.0.1'::ip", false))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void test_both_operands_are_ints() {
        assertThatThrownBy(() -> assertEvaluate("1 << 2", false))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void test_both_operands_are_of_wrong_type() {
        assertThatThrownBy(() -> assertEvaluate("1.2 << { cidr = '192.168.0.0/24'}", false))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageStartingWith("Unknown function: (1.2 << _map('cidr', '192.168.0.0/24')), " +
                "no overload found for matching argument types: (double precision, object).");
    }

    @Test
    public void test_ipv4_operands_are_correct() {
        assertEvaluate("'192.168.0.0'::ip << '192.168.0.1/24'", true);
        assertEvaluate("'192.168.0.255'::ip << '192.168.0.1/24'", true);
    }

    @Test
    public void test_ipv4_operands_are_correct_from_query() {
        assertEvaluate("name << name",
                       true,
                       Literal.of("192.168.0.255"),
                       Literal.of("192.168.0.1/24"));
    }

    @Test
    public void test_ipv4_both_operands_are_ips_from_query() {
        assertThatThrownBy(() -> assertEvaluate("ip << ip ",
                false,
                Literal.of("192.168.0.0"),
                Literal.of("192.168.0.1")))
            .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void test_ipv6_both_operands_are_text_from_query() {
        assertThatThrownBy(() -> assertEvaluate("name << name",
                true,
                Literal.of("2001:db8:0:0:0:0:0:c7/120"),
                Literal.of("2001:db8:0:0:0:0:0:c8/120")))
            .isExactlyInstanceOf(ConversionException.class);
    }
}
