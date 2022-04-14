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

import org.junit.Test;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class Concat_wsFunctionTest extends ScalarTestCase {

    @Test
    public void testMinimumArguments() {
        assertNormalize("concat_ws(',','foo')",
            isLiteral("foo"));
    }

    @Test
    public void testTwoStringArguments() {
        assertNormalize("concat_ws(',','foo','bar')",
            isLiteral("foo,bar"));
    }

    @Test
    public void test4Arguments(){
        assertNormalize("concat_ws(',','foo','bar','joe')",
            isLiteral("foo,bar,joe"));
    }

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
    public void testWithNullLast() {
        assertNormalize("concat_ws(',', NULL,'abcde', 2, NULL)",
            isLiteral("abcde,2"));
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
    public void testInvalidNullDelimiter(){
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage(
            "Delimiting argument of the 'concat_ws' function cannot be null."
        );
        assertNormalize("concat_ws(NULL, 'abcde','2')"
            ,null);
    }

    @Test
    public void testInvalidArrayArgument(){
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
            "Unknown function: concat_ws(',', 'foo', []), " +
                "no overload found for matching argument types: (text, text, undefined_array). " +
                "Possible candidates: concat_ws(text):text"
        );
        assertNormalize("concat_ws(',' , 'foo', [])"
            ,null);
    }
}
