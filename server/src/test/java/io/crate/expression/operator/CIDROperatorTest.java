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

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import org.junit.Test;

public class CIDROperatorTest extends ScalarTestCase {

    @Test
    public void test_ipv4_operands_in_wrong_order() {
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("'192.168.0.1/24' << '192.168.0.1'", false);
    }

    @Test
    public void test_ipv4_operands_in_wrong_order_from_query() {
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("name << ip",
                       false,
                       Literal.of("192.168.0.1/24"), Literal.of("192.168.0.1"));
    }

    @Test
    public void test_ipv6_operands_in_wrong_order() {
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("'2001:db8::1/120' << '2001:db8:0:0:0:0:0:c8'::ip", false);
    }

    @Test
    public void test_ipv6_operands_in_wrong_order_from_query() {
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("name << ip",
                       false,
                       Literal.of("2001:db8::1/120"), Literal.of("2001:db8:0:0:0:0:0:c8"));
    }

    @Test
    public void test_ipv4_both_operands_are_ips() {
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("'192.168.0.1'::ip << '192.168.0.1'::ip", false);
    }

    @Test
    public void test_both_operands_are_ints() {
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("1 << 2", false);
    }

    @Test
    public void test_both_operands_are_of_wrong_type() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: (1.2 << _map('cidr', '192.168.0.0/24'))," +
                                        " no overload found for matching argument types: (double precision, object).");
        assertEvaluate("1.2 << { cidr = '192.168.0.0/24'}", false);
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
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("ip << ip ",
                       false,
                       Literal.of("192.168.0.0"),
                       Literal.of("192.168.0.1"));
    }

    @Test
    public void test_ipv6_both_operands_are_text_from_query() {
        expectedException.expect(IllegalArgumentException.class);
        assertEvaluate("name << name",
                       true,
                       Literal.of("2001:db8:0:0:0:0:0:c7/120"),
                       Literal.of("2001:db8:0:0:0:0:0:c8/120"));
    }
}
