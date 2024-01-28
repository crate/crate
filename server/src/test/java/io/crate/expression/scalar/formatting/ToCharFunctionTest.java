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

package io.crate.expression.scalar.formatting;

import static io.crate.testing.Asserts.isNotSameInstance;
import static io.crate.testing.Asserts.isSameInstance;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;


public class ToCharFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluateTimestamp() {
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12.12345', 'Day,  DD  HH12:MI:SS')",
            "Thursday,  01  05:31:12"
        );
    }

    @Test
    public void test_lower_case_yyyy_supported() throws Exception {
        assertEvaluate("to_char(timestamp '1970-01-01', 'yyyy')", "1970");
        assertEvaluate("to_char(timestamp '1971-01-01T17:31:12', 'yyyy')", "1971");
        assertEvaluate("to_char(interval '2 year', 'yyyy')", "0002");
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds', 'yyyy')", "0001");
    }

    @Test
    public void testEvaluateTimestampWithNullPattern() {
        assertEvaluateNull("to_char(timestamp '1970-01-01T17:31:12', null)");
    }

    @Test
    public void testEvaluateNullExpression() {
        assertEvaluateNull("to_char(null, 'EEEE, LLLL d - h:m a uuuu G')");
    }

    @Test
    public void testEvaluateInterval() {
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds', 'YYYY MM DD HH12:MI:SS')", "0001 03 22 05:06:07");
    }

    @Test
    public void testEvaluateIntervalWithNullPattern() {
        assertEvaluateNull("to_char(timestamp '1970-01-01T17:31:12', null)");
    }

    @Test
    public void testCompileWithValues() throws Exception {
        assertCompile("to_char(timestamp, 'Day,  DD  HH12:MI:SS')", isNotSameInstance());
    }

    @Test
    public void testCompileWithRefs() throws Exception {
        assertCompile("to_char(timestamp, name)", isSameInstance());
    }
}
