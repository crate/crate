/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.scalar.arithmetic;

import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MapFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testMapWithWrongNumOfArguments() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("The number of arguments is incorrect");
        assertEvaluate("_map('foo', 1, 'bar')", null);
    }

    @Test
    public void testKeyNotOfTypeString() throws Exception {
        assertEvaluate("_map(10, 2)", Collections.singletonMap("10", 2L));
    }

    @Test
    public void testEvaluation() throws Exception {
        Map<String, Object> m = new HashMap<>();
        m.put("foo", 10L);
        // minimum args
        assertEvaluate("_map('foo', 10)", m);

        // variable args
        m.put("bar", new BytesRef("some"));
        assertEvaluate("_map('foo', 10, 'bar', 'some')", m);
    }
}
