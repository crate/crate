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

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;

public class ChrFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_value_returns_null() throws Exception {
        assertEvaluateNull("chr(null)");
    }

    @Test
    public void test_zero_value_throws_exception() throws Exception {
        assertThatThrownBy(() -> assertEvaluate("chr(0)", ""))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("null character not permitted");
    }

    @Test
    public void test_negative_number_throws_exception() throws Exception {
        assertThatThrownBy(() -> assertEvaluate("chr(-1)", ""))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("requested character too large for encoding: -1");
    }

    @Test
    public void test_large_number_throws_exception() throws Exception {
        assertThatThrownBy(() -> assertEvaluate("chr(1114112)", ""))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("requested character too large for encoding: 1114112");
    }

    @Test
    public void test_empty_value_throws_exception() throws Exception {
        assertThatThrownBy(() -> assertEvaluate("chr()", ""))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: chr(). Possible candidates: chr(integer):text");
    }

    @Test
    public void test_chr_positive_number() throws Exception {
        assertEvaluate("chr(65)", "A");
    }
}
