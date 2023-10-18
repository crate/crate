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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class OctalTest {

    @Test
    void testOnlyPrintableCharacters() {
        final byte[] expected = {97, 98, 99, 100};
        assertThat(Octal.decode("abcd")).isEqualTo(expected);
    }

    @Test
    void testValidEncodedString() {
        final byte[] expected = {48, 49, 50, 92, 51, 52, 53, 0, 1};
        assertThat(Octal.decode("012\\\\345\\000\\001")).isEqualTo(expected);
    }

    @Test
    void testIncompleteEscapeSequence() {
        assertThatThrownBy(() -> Octal.decode("abc\\"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Invalid escape sequence at index 3");
    }

    /**
     * Octal number should span 3 characters
     */
    @Test
    void testInvalidOctalNumber1() {
        assertThatThrownBy(() -> Octal.decode("\\00"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith("Invalid escape sequence at index 0");
    }

    @Test
    void testInvalidOctalNumber2() {
        assertThatThrownBy(() -> Octal.decode("\\008"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Illegal octal character 8 at index 3");
    }

    @Test
    void testEscapes() {
        // backslashes
        assertThat(Octal.decode("\\\\ \\134")).isEqualTo(new byte[] {92, 32, 92});
        // single quotes
        assertThat(Octal.decode("' \\047")).isEqualTo(new byte[] {39, 32, 39});
    }

    @Test
    void testEncode() {
        assertThat(Octal.encode("a\bb\\c".getBytes(StandardCharsets.UTF_8))).isEqualTo("a\\010b\\\\c");
    }

}
