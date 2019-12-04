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

package io.crate.expression.tablefunctions;

import org.junit.Test;

public class UnnestFunctionTest extends AbstractTableFunctionsTest {

    @Test
    public void test1Col1Row() throws Exception {
        assertExecute("unnest([1])", "1\n");
    }

    @Test
    public void test2MixedCols() throws Exception {
        assertExecute("unnest([1, 2], ['a', 'b'])", "1| a\n2| b\n");
    }

    @Test
    public void testFirstArrayShorter() throws Exception {
        assertExecute("unnest([1, 2], [1, 2, 3])", "1| 1\n2| 2\nNULL| 3\n");
    }

    @Test
    public void testSecondArrayShorter() throws Exception {
        assertExecute("unnest([1, 2, 3], [1, 2])", "1| 1\n2| 2\n3| NULL\n");
    }

    @Test
    public void testUnnestWithNullArgReturnsZeroRows() {
        assertExecute("unnest(null)", "");
    }

    @Test
    public void testUnnestWithNullArgAndArrayArg() {
        assertExecute("unnest(null, [1, null])",
            "NULL| 1\n" +
            "NULL| NULL\n");
    }
}
