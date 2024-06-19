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

public class StringSplitPartFunctionTest extends ScalarTestCase {

    @Test
    public void split_part_mid() {
        assertEvaluate("split_part('abc~@~def~@~ghi', '~@~', 2)", "def");
    }

    @Test
    public void split_part_first() {
        assertEvaluate("split_part('abc~@~def~@~ghi', '~@~', 1)", "abc");
    }

    @Test
    public void split_part_last() {
        assertEvaluate("split_part('abc~@~def~@~ghi', '~@~', 3)", "ghi");
    }

    @Test
    public void split_part_index_smaller_one_throws_exception() {
        assertThatThrownBy(() -> {
            assertEvaluate("split_part('abc~@~def~@~ghi', '~@~', 0)", "");

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("index in split_part must be greater than zero");
    }

    @Test
    public void split_part_index_greater_index() {
        assertEvaluate("split_part('abc~@~def~@~ghi', '~@~', 4)", "");
    }

    @Test
    public void text_is_null() {
        assertEvaluateNull("split_part(NULL, ',', 3)");
    }

    @Test
    public void delimiter_is_null() {
        assertEvaluateNull("split_part('abcdefg', NULL, 3)");
    }

    @Test
    public void field_is_null() {
        assertEvaluateNull("split_part('a,bc,de,fg', ',', NULL)");
    }

    @Test
    public void delimiter_is_empty_first() {
        assertEvaluate("split_part('abcdefg', '', 1)", "abcdefg");
    }

    @Test
    public void delimiter_is_empty_second() {
        assertEvaluate("split_part('abcdefg', '', 2)", "");
    }

    @Test
    public void repeating_delimiter_mid() {
        assertEvaluate("split_part('+++++++++++a+++b', '+++', 4)", "++a");
    }

    @Test
    public void repeating_delimiter_last() {
        assertEvaluate("split_part('+++++++++++a+++b', '+++', 5)", "b");
    }


}
