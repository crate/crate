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

package io.crate.expression.scalar;

import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isNull;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;

public class ConcatWsFunctionTest extends ScalarTestCase {

    @Test
    public void testManyStrings() {
        assertNormalize("concat_ws(',', '535 Mission St.', '14th floor', 'San Francisco', 'CA', '94105')",
            isLiteral("535 Mission St.,14th floor,San Francisco,CA,94105"));
    }

    @Test
    public void testWithNull() {
        assertNormalize("concat_ws(',', NULL,'abcde', 2, NULL, 22)",
            isLiteral("abcde,2,22"));
    }

    @Test
    public void testStringAndNumber() {
        assertNormalize("concat_ws('|','foo', 3)",
            isLiteral("foo|3"));
    }

    @Test
    public void testNumberAndString() {
        assertNormalize("concat_ws(';',3, 2, 'foo')",
            isLiteral("3;2;foo"));
    }

    @Test
    public void all_non_separator_args_are_nulls_returns_empty_string() {
        assertNormalize("concat_ws(',',null)",
            isLiteral(""));
    }

    @Test
    public void testNullSeparatorReturnsNull() {
        assertEvaluateNull("concat_ws(NULL, 'abcde','2')");
    }

    @Test
    public void testInvalidArrayArgument() {
        assertThatThrownBy(() -> assertNormalize("concat_ws(',' , 'foo', [])", isNull()))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessage("Unknown function: concat_ws(',', 'foo', []), " +
                        "no overload found for matching argument types: (text, text, undefined_array). " +
                        "Possible candidates: concat_ws(text):text");
    }
}
