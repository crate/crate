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

package io.crate.user.metadata;

import org.elasticsearch.test.ESTestCase;
import io.crate.user.SecureHash;
import org.elasticsearch.common.settings.SecureString;
import org.junit.Test;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class SecureHashTest extends ESTestCase {

    private static final SecureString PASSWORD =
        new SecureString("password".toCharArray());

    private static final SecureString INVALID_PASSWORD =
        new SecureString("invalid-password".toCharArray());

    @Test
    public void testSamePasswordsGenerateDifferentHash() throws Exception {
        SecureHash hash1 = SecureHash.of(PASSWORD);
        SecureHash hash2 = SecureHash.of(PASSWORD);
        assertNotEquals(hash1, hash2);
    }

    @Test
    public void testVerifyHash() throws Exception {
        SecureHash hash = SecureHash.of(PASSWORD);

        assertTrue(hash.verifyHash(PASSWORD));
        assertFalse(hash.verifyHash(INVALID_PASSWORD));
    }

    @Test
    public void testNonAsciiChars() throws InvalidKeySpecException, NoSuchAlgorithmException {
        SecureString pw = new SecureString("πä😉ـص".toCharArray());
        SecureHash hash = SecureHash.of(pw);

        assertTrue(hash.verifyHash(pw));
    }
}
