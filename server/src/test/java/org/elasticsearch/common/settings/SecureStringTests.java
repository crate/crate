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

package org.elasticsearch.common.settings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

import java.util.Arrays;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class SecureStringTests extends ESTestCase {

    @Test
    public void testCloseableCharsDoesNotModifySecureString() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        try (SecureString copy = secureString.clone()) {
            assertThat(copy.getChars()).isEqualTo(password);
            assertThat(copy.getChars()).isNotSameAs(password);
        }
        assertSecureStringEqualToChars(password, secureString);
    }

    @Test
    public void testClosingSecureStringDoesNotModifyCloseableChars() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        SecureString copy = secureString.clone();
        assertThat(copy.getChars()).isEqualTo(password);
        assertThat(copy.getChars()).isNotSameAs(password);
        final char[] passwordCopy = Arrays.copyOf(password, password.length);
        assertThat(passwordCopy).isEqualTo(password);
        secureString.close();
        assertThat(passwordCopy[0]).isNotEqualTo(password[0]);
        assertThat(copy.getChars()).isEqualTo(passwordCopy);
    }

    @Test
    public void testClosingChars() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        SecureString copy = secureString.clone();
        assertThat(copy.getChars()).isEqualTo(password);
        assertThat(copy.getChars()).isNotSameAs(password);
        copy.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            copy.close();
        }
        assertThatThrownBy(copy::getChars)
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already been closed");
    }

    @Test
    public void testGetCloseableCharsAfterSecureStringClosed() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        secureString.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            secureString.close();
        }
        assertThatThrownBy(secureString::clone)
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already been closed");
    }

    private void assertSecureStringEqualToChars(char[] expected, SecureString secureString) {
        int pos = 0;
        for (int i : secureString.chars().toArray()) {
            if (pos >= expected.length) {
                fail("Index " + i + " greated than or equal to array length " + expected.length);
            } else {
                assertThat((char) i).isEqualTo(expected[pos++]);
            }
        }
    }
}
