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

package io.crate.common;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class OctalTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testOnlyPrintableCharacters() {
        final byte[] expected = {97, 98, 99, 100};
        assertThat(Octal.decode("abcd"), is(expected));
    }

    @Test
    public void testValidEncodedString() {
        final byte[] expected = {48, 49, 50, 92, 51, 52, 53, 0, 1};
        assertThat(Octal.decode("012\\\\345\\000\\001"), is(expected));
    }

    @Test
    public void testIncompleteEscapeSequence() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(containsString("Invalid escape sequence at index 3"));
        Octal.decode("abc\\");
    }

    /**
     * Octal number should span 3 characters
     */
    @Test
    public void testInvalidOctalNumber1() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(containsString("Invalid escape sequence at index 0"));
        Octal.decode("\\00");
    }

    @Test
    public void testInvalidOctalNumber2() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal octal character 8 at index 3");
        Octal.decode("\\008");
    }

    @Test
    public void testEscapes() {
        // backslashes
        assertThat(Octal.decode("\\\\ \\134"), is(new byte[]{92, 32, 92}));
        // single quotes
        assertThat(Octal.decode("' \\047"), is(new byte[]{39, 32, 39}));
    }

    @Test
    public void testEncode() {
        assertThat(Octal.encode("a\bb\\c".getBytes(StandardCharsets.UTF_8)), is("a\\010b\\\\c"));
    }

}
