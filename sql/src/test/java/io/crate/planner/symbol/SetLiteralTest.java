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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.TestCase.assertSame;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SetLiteralTest {

    public static SetLiteral stringSet(String... items) {
        ImmutableSet.Builder<BytesRef> builder = ImmutableSet.builder();
        for (String item : items) {
            builder.add(new BytesRef(item));
        }
        return new SetLiteral(DataType.STRING, builder.build());
    }

    @Test
    public void testIntersection() throws Exception {
        SetLiteral setLiteral1 = stringSet("alpha", "bravo");
        SetLiteral setLiteral2 = stringSet("foo", "alpha");
        assertEquals(stringSet("alpha"), setLiteral1.intersection(setLiteral2));
        assertEquals(stringSet("alpha"), setLiteral2.intersection(setLiteral1));
    }

    @Test
    public void testEmptyIntersection() throws Exception {
        SetLiteral setLiteral1 = stringSet();
        SetLiteral setLiteral2 = stringSet("foo", "alpha");
        assertSame(setLiteral1, setLiteral2.intersection(setLiteral1));
    }

    @Test
    public void testCompareTo1() {
        SetLiteral setLiteral1 = stringSet("alpha", "bravo", "charlie", "delta");
        SetLiteral setLiteral2 = stringSet("foo", "alpha", "beta", "bar");
        assertEquals(0, setLiteral1.compareTo(setLiteral2));
    }

    @Test
    public void testCompareTo2() {
        SetLiteral setLiteral1 = stringSet("alpha", "bravo", "charlie", "delta");
        SetLiteral setLiteral2 = stringSet("foo", "alpha", "beta");
        assertEquals(1, setLiteral1.compareTo(setLiteral2));
    }

    @Test
    public void testCompareTo3() {
        SetLiteral setLiteral1 = stringSet("alpha", "bravo");
        SetLiteral setLiteral2 = stringSet("foo", "alpha", "beta");
        assertEquals(-1, setLiteral1.compareTo(setLiteral2));
    }

    @Test
    public void testSerialization() throws IOException {
        SetLiteral setLiteralOut = stringSet("alpha", "bravo", "charlie", "delta");

        BytesStreamOutput outputStream = new BytesStreamOutput();
        setLiteralOut.writeTo(outputStream);

        BytesStreamInput inputStream = new BytesStreamInput(outputStream.bytes().copyBytesArray());
        SetLiteral setLiteralIn = SetLiteral.FACTORY.newInstance();
        setLiteralIn.readFrom(inputStream);

        assertEquals(setLiteralOut.valueType(), setLiteralIn.valueType());
        assertEquals(setLiteralOut.literals(), setLiteralIn.literals());
    }

    @Test
    public void testConvertTo() throws Exception {
        SetLiteral intSet = new SetLiteral(DataType.INTEGER, Sets.newHashSet(1, 2, 3, 4));

        Literal literal = intSet.convertTo(DataType.LONG_SET);
        assertThat(literal.valueType(), is(DataType.LONG_SET));
    }

    @Test
    public void testConvertToWithNull() throws Exception {
        SetLiteral intSet = new SetLiteral(DataType.INTEGER, Sets.newHashSet(null, 2, 3, 4));

        Literal literal = intSet.convertTo(DataType.LONG_SET);
        assertThat(literal.valueType(), is(DataType.LONG_SET));
    }
}
