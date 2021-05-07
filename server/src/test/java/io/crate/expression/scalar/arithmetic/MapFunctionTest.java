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

package io.crate.expression.scalar.arithmetic;

import io.crate.expression.scalar.ScalarTestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MapFunctionTest extends ScalarTestCase {

    @Test
    public void testMapWithWrongNumOfArguments() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Unknown function: _map('foo', 1, 'bar')," +
                                        " no overload found for matching argument types: (text, integer, text).");
        assertEvaluate("_map('foo', 1, 'bar')", null);
    }

    @Test
    public void testKeyNotOfTypeString() {
        assertEvaluate("_map(10, 2)", Collections.singletonMap("10", 2));
    }

    @Test
    public void testEvaluation() {
        Map<String, Object> m = new HashMap<>();
        m.put("foo", 10);
        // minimum args
        assertEvaluate("_map('foo', 10)", m);

        // variable args
        m.put("bar", "some");
        assertEvaluate("_map('foo', 10, 'bar', 'some')", m);
    }
}
