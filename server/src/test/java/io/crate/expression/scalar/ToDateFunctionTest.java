/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.time.format.DateTimeParseException;


public class ToDateFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluateDate() {
        assertEvaluate(
            "to_date('Friday, January 2 1970', 'EEEE, LLLL d yyyy')",
            86400000L
        );
    }

    @Test
    public void testEvaluateEmptyDate() {
        expectedException.expect(DateTimeParseException.class);
        expectedException.expectMessage("Text '' could not be parsed at index 0");
        assertEvaluate(
            "to_date('', 'EEEE, LLLL d')",
            null
        );
    }

    @Test
    public void testEvaluateNullDate() {
        assertEvaluate(
            "to_date(null, 'EEEE, LLLL d - h:m a uuuu G')",
            null
        );
    }

    @Test
    public void testEvaluateEmptyPattern() {
        expectedException.expect(DateTimeParseException.class);
        expectedException.expectMessage("Text 'Thursday, January 1' could not be parsed, unparsed text found at index 0");
        assertEvaluate(
            "to_date('Thursday, January 1', '')",
            null
        );
    }

    @Test
    public void testEvaluateNullPattern() {
        assertEvaluate(
            "to_date('Thursday, January 1', null)",
            null
        );
    }


}
