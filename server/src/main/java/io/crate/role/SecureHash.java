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

package io.crate.role;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Objects;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * Secure Password Hashing that uses the PBKDF2 Algorithm
 * @see <a href="https://tools.ietf.org/html/rfc2898">PBKDF2 Algorithm</a>
 */
public final class SecureHash implements Writeable {

    private static final String X_CONTENT_KEY_ITERATIONS = "iterations";
    private static final String X_CONTENT_KEY_SALT = "salt";
    private static final String X_CONTENT_KEY_HASH = "hash";

    private static final String ALGORITHM = "PBKDF2WithHmacSHA512";
    private static final int HASH_BIT_LENGTH = 64;
    private static final int DEFAULT_ITERATIONS = 40_000;

    private final int iterations;
    private final byte[] hash;
    private final byte[] salt;

    private SecureHash(byte[] salt, byte[] hash) {
        this(DEFAULT_ITERATIONS, salt, hash);
    }

    private SecureHash(int iterations, byte[] salt, byte[] hash) {
        this.iterations = iterations;
        this.salt = salt;
        this.hash = hash;
    }

    public static SecureHash of(SecureString password) throws NoSuchAlgorithmException, InvalidKeySpecException {
        SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
        byte[] salt = new byte[32];
        random.nextBytes(salt);
        byte[] hash = pbkdf2(password, salt, DEFAULT_ITERATIONS, HASH_BIT_LENGTH);
        return new SecureHash(salt, hash);
    }

    public static SecureHash of(int iterations, byte[] salt, byte[] hash) {
        return new SecureHash(iterations, salt, hash);
    }

    /**
     * Validates the password against the existing hash
     *
     * @param password the clear-text password
     * @return true: password is correct
     *         false: password not correct
     */
    public boolean verifyHash(SecureString password) {
        try {
            byte[] testHash = pbkdf2(password, salt, iterations, HASH_BIT_LENGTH);
            return slowEquals(hash, testHash);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            return false;
        }
    }

    /**
     * Compares two byte arrays in length-constant time to avoid timing attacks.
     * @param a first byte array
     * @param b second byte array
     * @return true if arrays are equal, otherwise false
     */
    private static boolean slowEquals(byte[] a, byte[] b) {
        int diff = a.length ^ b.length;
        for (int i = 0; i < a.length && i < b.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }

    /**
     * Generates the salted PBKDF2 hash of a clear-text password
     *
     * @param password   The clear-text password of type {@link SecureString} to allow clearing after hash generation.
     * @param salt       Random generated bytes to prevent against attacks using rainbow tables
     * @param iterations The number of times that the password is hashed during the derivation of the symmetric key.
     *                   The higher number, the more difficult it is to brute force
     * @param length     Bit-length of the derived symmetric key.
     * @return
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeySpecException
     */
    private static byte[] pbkdf2(SecureString password, byte[] salt, int iterations, int length) throws NoSuchAlgorithmException, InvalidKeySpecException {
        PBEKeySpec spec = new PBEKeySpec(password.getChars(), salt, iterations, length * 8);
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(ALGORITHM);
        byte[] hash = secretKeyFactory.generateSecret(spec).getEncoded();
        spec.clearPassword();
        return hash;
    }

    public static SecureHash readFrom(StreamInput in) throws IOException {
        int iterations = in.readInt();
        byte[] hash = in.readByteArray();
        byte[] salt = in.readByteArray();
        return new SecureHash(iterations, salt, hash);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(iterations);
        out.writeByteArray(hash);
        out.writeByteArray(salt);
    }

    public static SecureHash fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token currentToken;
        int iterations = 0;
        byte[] hash = new byte[0];
        byte[] salt = new byte[0];
        boolean hasPassword = false;

        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            while (currentToken == XContentParser.Token.FIELD_NAME) {
                hasPassword = true;
                String currentFieldName = parser.currentName();
                currentToken = parser.nextToken();
                switch (currentFieldName) {
                    case X_CONTENT_KEY_ITERATIONS:
                        if (currentToken != XContentParser.Token.VALUE_NUMBER) {
                            throw new ElasticsearchParseException(
                                "failed to parse SecureHash, 'iterations' value is not a number [{}]", currentToken);
                        }
                        iterations = parser.intValue();
                        break;
                    case X_CONTENT_KEY_HASH:
                        if (currentToken.isValue() == false) {
                            throw new ElasticsearchParseException(
                                "failed to parse SecureHash, 'hash' does not contain any value [{}]", currentToken);
                        }
                        hash = parser.binaryValue();
                        break;
                    case X_CONTENT_KEY_SALT:
                        if (currentToken.isValue() == false) {
                            throw new ElasticsearchParseException(
                                "failed to parse SecureHash, 'salt' does not contain any value [{}]", currentToken);
                        }
                        salt = parser.binaryValue();
                        break;
                    default:
                        throw new ElasticsearchParseException("failed to parse secure_hash");
                }
            }
        }
        if (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            throw new ElasticsearchParseException("failed to parse secure_hash, expected an object token at the end");
        }

        if (hasPassword) {
            return SecureHash.of(iterations, salt, hash);
        }
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(iterations, Arrays.hashCode(salt), Arrays.hashCode(hash));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SecureHash that = (SecureHash) o;

        if (iterations != that.iterations) return false;
        if (!Arrays.equals(hash, that.hash)) return false;
        return Arrays.equals(salt, that.salt);
    }
}
