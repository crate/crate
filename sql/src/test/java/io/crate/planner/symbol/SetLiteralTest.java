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

import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class SetLiteralTest {

    @Test
    public void testCompareTo1() {
        SetLiteral setLiteral1 = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("alpha"),
                                new StringLiteral("bravo"),
                                new StringLiteral("charlie"),
                                new StringLiteral("delta")
                        )
                )
        );

        SetLiteral setLiteral2 = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("foo"),
                                new StringLiteral("alpha"),
                                new StringLiteral("beta"),
                                new StringLiteral("bar")
                        )
                )
        );

        assertEquals(0, setLiteral1.compareTo(setLiteral2.literals()));
    }

    @Test
    public void testCompareTo2() {
        SetLiteral setLiteral1 = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("alpha"),
                                new StringLiteral("bravo"),
                                new StringLiteral("charlie"),
                                new StringLiteral("delta")
                        )
                )
        );

        SetLiteral setLiteral2 = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("foo"),
                                new StringLiteral("alpha"),
                                new StringLiteral("beta")
                        )
                )
        );

        assertEquals(1, setLiteral1.compareTo(setLiteral2.literals()));
    }

    @Test
    public void testCompareTo3() {
        SetLiteral setLiteral1 = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("charlie"),
                                new StringLiteral("delta")
                        )
                )
        );

        SetLiteral setLiteral2 = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("foo"),
                                new StringLiteral("alpha"),
                                new StringLiteral("beta")
                        )
                )
        );

        assertEquals(-1, setLiteral1.compareTo(setLiteral2.literals()));
    }

    @Test
    public void testCompareTo4() {
        SetLiteral setLiteral1 = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("a"),
                                new StringLiteral("b"),
                                new StringLiteral("c"),
                                new StringLiteral("d"),
                                new StringLiteral("e")
                        )
                )
        );

        SetLiteral setLiteral2 = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("a"),
                                new StringLiteral("b"),
                                new StringLiteral("c"),
                                new StringLiteral("e"),
                                new StringLiteral("f")
                        )
                )
        );

        assertEquals(0, setLiteral1.compareTo(setLiteral2.literals()));
    }

    @Test
    public void testSerialization() throws IOException {
        BytesStreamOutput outputStream = new BytesStreamOutput();

        SetLiteral setLiteralOut = new SetLiteral(
                DataType.STRING,
                new HashSet<Literal>(
                        Arrays.asList(
                                new StringLiteral("alpha"),
                                new StringLiteral("bravo"),
                                new StringLiteral("charlie"),
                                new StringLiteral("delta")
                        )
                )
        );

        setLiteralOut.writeTo(outputStream);

        BytesStreamInput inputStream = new BytesStreamInput(outputStream.bytes().copyBytesArray());
        SetLiteral setLiteralIn = SetLiteral.FACTORY.newInstance();
        setLiteralIn.readFrom(inputStream);

        assertEquals(setLiteralOut.valueType(), setLiteralIn.valueType());
        assertEquals(setLiteralOut.literals(), setLiteralIn.literals());
    }
}
