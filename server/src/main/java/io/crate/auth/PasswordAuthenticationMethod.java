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

package io.crate.auth;

import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.elasticsearch.common.settings.SecureString;
import org.jspecify.annotations.Nullable;

import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.SecureHash;

public class PasswordAuthenticationMethod implements AuthenticationMethod {

    public static final String NAME = "password";

    private static final String DIGEST_ALGORITHM = "HmacSHA256";

    // HmacSHA256 outputs 256 bits, 32 bytes.
    // Taking 32 bytes as key length as per RFC recommendation.
    // From https://datatracker.ietf.org/doc/html/rfc4868:
    // key lengths less than the output length
    // decrease security strength, and keys longer than the output length do
    // not significantly increase security strength.
    // Shared by all threads (immutable), so digests are comparable across threads.
    private static final SecretKeySpec DIGEST_KEY;

    static {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        DIGEST_KEY = new SecretKeySpec(key, DIGEST_ALGORITHM);
    }

    // Mac is not thread safe, created once per thread and re-used.
    private static final ThreadLocal<Mac> MAC = ThreadLocal.withInitial(() -> {
        try {
            Mac mac = Mac.getInstance(DIGEST_ALGORITHM);
            mac.init(DIGEST_KEY);
            return mac;
        } catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    });

    private final Roles roles;

    static class PasswordAuthError extends RuntimeException {

        PasswordAuthError(String message) {
            super(message);
        }


        @Override
        public synchronized Throwable fillInStackTrace() {
            // no need for stack trace, thrown only here
            return this;
        }
    }

    PasswordAuthenticationMethod(Roles roles) {
        this.roles = roles;
    }

    private record PasswordAuthToken(Role role,
                                     String username,
                                     SecureHash acceptedHash,
                                     byte[] credentialsDigest) implements AuthToken { }

    @Override
    public AuthToken authenticate(Credentials credentials, ConnectionProperties connProperties) {
        var username = credentials.username();
        var password = credentials.password();
        assert username != null : "User name must be not null on password authentication method";
        Role user = roles.findUser(username);
        if (user != null && password != null && !password.isEmpty()) {
            SecureHash secureHash = user.password();
            if (secureHash != null && secureHash.verifyHash(password)) {
                // We never compute same digest twice:
                // Either renew got a valid cached user and authenticate is not called,
                // or password was updated, and then we have to compute new digest.
                return new PasswordAuthToken(user, username, secureHash, digest(password));
            }
        }
        throw new PasswordAuthError("password authentication failed for user \"" + username + "\"");
    }

    @Nullable
    @Override
    public AuthToken renew(AuthToken token, Credentials credentials) {
        if (!(token instanceof PasswordAuthToken pwdAuthToken)) {
            // AuthToken belongs to different auth method
            return null;
        }
        String username = credentials.username();
        if (pwdAuthToken.username().equals(username) == false) {
            return null;
        }
        byte[] credentialsDigest = digest(credentials.password());
        if (MessageDigest.isEqual(pwdAuthToken.credentialsDigest(), credentialsDigest) == false) { // Null safe
            return null;
        }
        // Ensure user still exists and password hasn't changed
        Role user = roles.findUser(username);
        if (user == null) {
            return null;
        }
        SecureHash currentSecureHash = user.password();
        if (currentSecureHash == null || currentSecureHash.equals(pwdAuthToken.acceptedHash()) == false) {
            return null;
        }
        return new PasswordAuthToken(user, username, currentSecureHash, credentialsDigest);
    }

    private static byte @Nullable [] digest(@Nullable SecureString password) {
        if (password == null) {
            return null;
        }
        Mac mac = MAC.get();
        char[] chars = password.getChars();
        for (int i = 0; i < chars.length; i++) {
            mac.update((byte) (chars[i] >> 8)); // high byte
            mac.update((byte) chars[i]); // low byte
        }
        // Resets state, safe to re-use for the next time.
        return mac.doFinal();
    }

    @Override
    public String name() {
        return NAME;
    }
}
