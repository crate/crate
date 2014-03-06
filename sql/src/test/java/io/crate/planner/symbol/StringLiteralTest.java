/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.symbol;

import org.apache.lucene.util.BytesRef;
import io.crate.DataType;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;


public class StringLiteralTest {

    @Test
    public void testConvertTo() {
        StringLiteral stringLiteral = new StringLiteral("123");

        assertEquals(123L, stringLiteral.convertTo(DataType.LONG).value());
        assertEquals(123, stringLiteral.convertTo(DataType.INTEGER).value());
        assertEquals(123.0d, stringLiteral.convertTo(DataType.DOUBLE).value());
        assertEquals(123.0f, stringLiteral.convertTo(DataType.FLOAT).value());
        assertEquals(123, stringLiteral.convertTo(DataType.SHORT).value());
        assertEquals(123, stringLiteral.convertTo(DataType.BYTE).value());
        assertEquals(new BytesRef("123"), stringLiteral.convertTo(DataType.STRING).value());

        assertEquals(false, new StringLiteral("false").convertTo(DataType.BOOLEAN).value());
        assertEquals(false, new StringLiteral("FALSE").convertTo(DataType.BOOLEAN).value());
        assertEquals(false, new StringLiteral("f").convertTo(DataType.BOOLEAN).value());
        assertEquals(false, new StringLiteral("F").convertTo(DataType.BOOLEAN).value());
        assertEquals(true, new StringLiteral("true").convertTo(DataType.BOOLEAN).value());
        assertEquals(true, new StringLiteral("TRUE").convertTo(DataType.BOOLEAN).value());
        assertEquals(true, new StringLiteral("t").convertTo(DataType.BOOLEAN).value());
        assertEquals(true, new StringLiteral("T").convertTo(DataType.BOOLEAN).value());
    }

    @Test
    public void testConvertToTimestamp() {
        StringLiteral stringLiteral = new StringLiteral("2014-02-28T02:39:33");
        assertEquals(1393555173000L, stringLiteral.convertTo(DataType.TIMESTAMP).value());

        stringLiteral = new StringLiteral("2014-02-28");
        assertEquals(1393545600000L, stringLiteral.convertTo(DataType.TIMESTAMP).value());
    }

    @Test(expected = NumberFormatException.class)
    public void testConvertToUnsupportedNumberConversion() {
        StringLiteral stringLiteral = new StringLiteral("hello");
        stringLiteral.convertTo(DataType.LONG).value();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConvertToUnsupportedBooleanConversion() {
        StringLiteral stringLiteral = new StringLiteral("hello");
        stringLiteral.convertTo(DataType.BOOLEAN).value();
    }
}
