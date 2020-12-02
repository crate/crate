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

import io.crate.exceptions.InvalidArgumentException;
import org.junit.Test;

import java.text.ParseException;
import java.time.format.DateTimeParseException;


public class ToNumberFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testEvaluateNumber() {
        assertEvaluate("to_number('1234,456.78', '#####')", 1234L);
        assertEvaluate("to_number('1,234,456', '#,###,###')", 1234456L);
        assertEvaluate("to_number('1,234,456.78', '#,###,###.#')", 1234456.78);
        assertEvaluate("to_number('301%', '###%')", 3.01);
    }

    @Test
    public void testEvaluateEmptyNumber() {
        expectedException.expect(InvalidArgumentException.class);
        expectedException.expectMessage("Unparseable number: \"\"");
        assertEvaluate("to_number('', '###%')", null);
    }

    @Test
    public void testEvaluateNullNumber() {
        assertEvaluate("to_number(null, '###%')", null);
    }

    @Test
    public void testEvaluateEmptyPattern() {
        assertEvaluate("to_number('301%', '')", 301L);
    }

    @Test
    public void testEvaluateNullPattern() {
        assertEvaluate("to_number('301%', null)", null);
    }


}
