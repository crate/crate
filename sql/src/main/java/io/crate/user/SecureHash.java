/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.user;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;


/**
 * Taken from mibe's PR
 * Subject to change!
 */
public final class SecureHash implements Writeable, ToXContent {

    private static final String ALGORITHM = "PBKDF2WithHmacSHA256";
    private static final int HASH_BIT_LENGTH = 32;
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
        byte[] salt = new byte[32];
        SecureRandom rand = new SecureRandom();
        rand.nextBytes(salt);
        byte[] hash = pbkdf2(password, salt, DEFAULT_ITERATIONS, HASH_BIT_LENGTH);
        return new SecureHash(salt, hash);
    }

    public boolean verifyHash(SecureString password) {
        if (password == null || password.length() == 0) {
            return false;
        }
        try {
            byte[] testHash = pbkdf2(password, salt, iterations, HASH_BIT_LENGTH);
            return slowEquals(hash, testHash);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            return false;
        }
    }

    private static boolean slowEquals(byte[] a, byte[] b) {
        int diff = a.length ^ b.length;
        for (int i = 0; i < a.length && i < b.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }

    private static byte[] pbkdf2(SecureString password, byte[] salt, int iterations, int length) throws NoSuchAlgorithmException, InvalidKeySpecException {
        PBEKeySpec spec = new PBEKeySpec(password.getChars(), salt, iterations, length * 8);
        SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(ALGORITHM);
        byte[] hash = secretKeyFactory.generateSecret(spec).getEncoded();
        spec.clearPassword();
        return hash;
    }

    private static SecureHash readFrom(StreamInput in) throws IOException {
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("secure_hash")
            .field("iterations", iterations)
            .field("hash", hash)
            .field("salt", salt)
            .endObject();
        return builder;
    }
}
