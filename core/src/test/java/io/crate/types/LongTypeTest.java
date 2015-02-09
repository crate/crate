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

package io.crate.types;

import org.apache.lucene.util.BytesRef;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class LongTypeTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testBytesRefToLongParsing() throws Exception {
        assertBytesRefParsing("12839", 12839L);
        assertBytesRefParsing("-12839", -12839L);
        assertBytesRefParsing(Long.toString(Long.MAX_VALUE), Long.MAX_VALUE);
        assertBytesRefParsing(Long.toString(Long.MIN_VALUE), Long.MIN_VALUE);

        assertBytesRefParsing("+2147483647111", 2147483647111L);
    }

    @Test
    public void testConversionWithNonAsciiCharacter() throws Exception {
        expectedException.expect(NumberFormatException.class);
        expectedException.expectMessage("\u03C0"); // "π" GREEK SMALL LETTER PI
        assertBytesRefParsing("\u03C0", 0L);
    }

    @Test
    public void testInvalidFirstChar() throws Exception {
        expectedException.expect(NumberFormatException.class);
        assertBytesRefParsing(" 1", 1L);
    }

    @Test
    public void testOnlyMinusSign() throws Exception {
        expectedException.expect(NumberFormatException.class);
        assertBytesRefParsing("-", 1L);
    }

    @Test
    public void testOnlyPlusSign() throws Exception {
        expectedException.expect(NumberFormatException.class);
        assertBytesRefParsing("+", 1L);
    }

    @Test
    public void testNumberThatIsGreaterThanMaxValue() throws Exception {
        expectedException.expect(NumberFormatException.class);
        assertBytesRefParsing(Long.toString(Long.MAX_VALUE) + "111", Long.MIN_VALUE);
    }

    private void assertBytesRefParsing(String s, long l) {
        assertThat(LongType.INSTANCE.value(new BytesRef(s)), is(l));
    }
}