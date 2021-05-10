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

import io.crate.expression.scalar.ScalarTestCase;
import org.hamcrest.core.IsSame;
import org.junit.Test;

import static org.hamcrest.Matchers.not;


public class ToCharFunctionTest extends ScalarTestCase {

    @Test
    public void testEvaluateTimestamp() {
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12.12345', 'Day,  DD  HH12:MI:SS')",
            "Thursday,  01  05:31:12"
        );
    }

    @Test
    public void testEvaluateTimestampWithNullPattern() {
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12', null)",
            null
        );
    }

    @Test
    public void testEvaluateNullExpression() {
        assertEvaluate(
            "to_char(null, 'EEEE, LLLL d - h:m a uuuu G')",
            null
        );
    }

    @Test
    public void testEvaluateInterval() {
        assertEvaluate("to_char(INTERVAL '1 year 2 months 3 weeks 5 hours 6 minutes 7 seconds', 'YYYY MM DD HH12:MI:SS')", "0001 03 22 05:06:07");
    }

    @Test
    public void testEvaluateIntervalWithNullPattern() {
        assertEvaluate(
            "to_char(timestamp '1970-01-01T17:31:12', null)",
            null
        );
    }

    @Test
    public void testCompileWithValues() throws Exception {
        assertCompile("to_char(timestamp, 'Day,  DD  HH12:MI:SS')", (s) -> not(IsSame.sameInstance(s)));
    }

    @Test
    public void testCompileWithRefs() throws Exception {
        assertCompile("to_char(timestamp, name)", IsSame::sameInstance);
    }
}
