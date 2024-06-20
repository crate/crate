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

package io.crate.expression.scalar.string;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class StringPaddingFunctionTest extends ScalarTestCase {

    @Test
    public void test_lpad_parameter_len_too_big() throws Exception {
        assertThatThrownBy(() -> assertEvaluateNull("lpad('yes', 2000000, 'yes')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("len argument exceeds predefined limit of 50000");
        assertThatThrownBy(() -> assertEvaluateNull("lpad('yes', a, 'yes')", Literal.of(2000000)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("len argument exceeds predefined limit of 50000");
    }

    @Test
    public void test_lpad_parameters_are_null() throws Exception {
        assertEvaluateNull("lpad(null, 5, '')");
        assertEvaluateNull("lpad(name, 5, '')", Literal.of((String) null));
        assertEvaluateNull("lpad('', null, '')");
        assertEvaluateNull("lpad('', a, '')", Literal.of((Integer) null));
        assertEvaluateNull("lpad('', 5, null)");
        assertEvaluateNull("lpad('', 5, name)", Literal.of((String) null));
    }

    @Test
    public void test_lpad_both_string_parameters_are_empty() throws Exception {
        assertEvaluate("lpad('', 5, '')", "");
        assertEvaluate("lpad(name, 5, name)", "", Literal.of(""), Literal.of(""));
    }

    @Test
    public void test_lpad_len_parameter_is_zero_or_less() throws Exception {
        assertEvaluate("lpad('yes', 0, 'yes')", "");
        assertEvaluate("lpad('yes', -1, 'yes')", "");
        assertEvaluate("lpad('yes', a, 'yes')", "", Literal.of(0));
        assertEvaluate("lpad('yes', a, 'yes')", "", Literal.of(-1));
    }

    @Test
    public void test_lpad_fill_parameter_is_empty() throws Exception {
        assertEvaluate("lpad('yes', 5, '')", "yes");
        assertEvaluate("lpad('yes', 5, name)", "yes", Literal.of(""));
        assertEvaluate("lpad('yes', 2, '')", "ye");
        assertEvaluate("lpad('yes', 2, name)", "ye", Literal.of(""));
    }

    @Test
    public void test_lpad_default_fill() throws Exception {
        assertEvaluate("lpad('yes', 1)", "y");
        assertEvaluate("lpad('yes', 3)", "yes");
        assertEvaluate("lpad('yes', 1)", "y");
        assertEvaluate("lpad('yes', 5)", "  yes");
    }

    @Test
    public void test_lpad_function() throws Exception {
        assertEvaluate("lpad('', 5, 'yes')", "yesye");
        assertEvaluate("lpad('', 5, name)", "yesye", Literal.of("yes"));
        assertEvaluate("lpad(name, 5, 'yes')", "yesye", Literal.of(""));
        assertEvaluate("lpad('', 5, 'yes')", "yesye", Literal.of(5));

        assertEvaluate("lpad('yes', 3, 'yes')", "yes");
        assertEvaluate("lpad('yes', 3, name)", "yes", Literal.of("yes"));
        assertEvaluate("lpad(name, 3, 'yes')", "yes", Literal.of("yes"));
        assertEvaluate("lpad('yes', a, 'yes')", "yes", Literal.of(3));

        assertEvaluate("lpad('yes', 1, 'yes')", "y");
        assertEvaluate("lpad('yes', 1, name)", "y", Literal.of("yes"));
        assertEvaluate("lpad(name, 1, 'yes')", "y", Literal.of("yes"));
        assertEvaluate("lpad('yes', a, 'yes')", "y", Literal.of(1));
    }

    @Test
    public void test_rpad_parameter_len_too_big() throws Exception {
        assertThatThrownBy(() -> assertEvaluateNull("rpad('yes', 2000000, 'yes')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("len argument exceeds predefined limit of 50000");

        assertThatThrownBy(() -> assertEvaluateNull("rpad('yes', a, 'yes')", Literal.of(2000000)))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("len argument exceeds predefined limit of 50000");
    }

    @Test
    public void test_rpad_parameters_are_null() throws Exception {
        assertEvaluateNull("rpad(null, 5, '')");
        assertEvaluateNull("rpad('', null, '')");
        assertEvaluateNull("rpad('', 5, null)");
        assertEvaluateNull("rpad(name, 5, '')", Literal.of((String) null));
        assertEvaluateNull("rpad('', a, '')", Literal.of((Integer) null));
        assertEvaluateNull("rpad('', 5, name)", Literal.of((String) null));
    }

    @Test
    public void test_rpad_both_string_parameters_are_empty() throws Exception {
        assertEvaluate("rpad('', 5, '')", "");
        assertEvaluate("rpad(name, 5, name)", "", Literal.of(""), Literal.of(""));
    }

    @Test
    public void test_rpad_len_parameter_is_zero_or_less() throws Exception {
        assertEvaluate("rpad('yes', 0, 'yes')", "");
        assertEvaluate("rpad('yes', -1, 'yes')", "");
        assertEvaluate("rpad('yes', a, 'yes')", "", Literal.of(0));
        assertEvaluate("rpad('yes', a, 'yes')", "", Literal.of(-1));
    }

    @Test
    public void test_rpad_fill_parameter_is_empty() throws Exception {
        assertEvaluate("rpad('yes', 5, '')", "yes");
        assertEvaluate("rpad('yes', 5, name)", "yes", Literal.of(""));
        assertEvaluate("rpad('yes', 2, '')", "ye");
        assertEvaluate("rpad('yes', 2, name)", "ye", Literal.of(""));
    }

    @Test
    public void test_rpad_default_fill() throws Exception {
        assertEvaluate("rpad('yes', 1)", "y");
        assertEvaluate("rpad('yes', 3)", "yes");
        assertEvaluate("rpad('yes', 1)", "y");
        assertEvaluate("rpad('yes', 5)", "yes  ");
    }

    @Test
    public void test_rpad_function() {
        assertEvaluate("rpad('', 5, 'yes')", "yesye");
        assertEvaluate("rpad('', 5, name)", "yesye", Literal.of("yes"));
        assertEvaluate("rpad(name, 5, 'yes')", "yesye", Literal.of(""));
        assertEvaluate("rpad('', a, 'yes')", "yesye", Literal.of(5));

        assertEvaluate("rpad('yes', 3, 'yes')", "yes");
        assertEvaluate("rpad('yes', 3, name)", "yes", Literal.of("yes"));
        assertEvaluate("rpad(name, 3, 'yes')", "yes", Literal.of("yes"));
        assertEvaluate("rpad('yes', a, 'yes')", "yes", Literal.of(3));

        assertEvaluate("rpad('yes', 1, 'yes')", "y");
        assertEvaluate("rpad('yes', 1, name)", "y", Literal.of("yes"));
        assertEvaluate("rpad(name, 1, 'yes')", "y", Literal.of("yes"));
        assertEvaluate("rpad('yes', a, 'yes')", "y", Literal.of(1));
    }
}
