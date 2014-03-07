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


public class BooleanLiteralTest {

    @Test
    public void testConvertTo() {
        Literal literal = new BooleanLiteral(true);

        assertEquals(new BytesRef("t"), literal.convertTo(DataType.STRING).value());
        assertEquals(true, literal.convertTo(DataType.BOOLEAN).value());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToLong() {
        Literal literal = new BooleanLiteral(true);
        literal.convertTo(DataType.LONG).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToInteger() {
        Literal literal = new BooleanLiteral(true);
        literal.convertTo(DataType.INTEGER).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToDouble() {
        Literal literal = new BooleanLiteral(true);
        literal.convertTo(DataType.DOUBLE).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToFloat() {
        Literal literal = new BooleanLiteral(true);
        literal.convertTo(DataType.FLOAT).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToShort() {
        Literal literal = new BooleanLiteral(true);
        literal.convertTo(DataType.SHORT).value();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConvertToByte() {
        Literal literal = new BooleanLiteral(true);
        literal.convertTo(DataType.BYTE).value();
    }

}
