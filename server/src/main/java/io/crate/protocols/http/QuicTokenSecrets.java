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

package io.crate.protocols.http;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpTransportSettings;

public final class QuicTokenSecrets {

    static final int MIN_SECRET_BYTES = 16;
    private static final int GENERATED_SECRET_BYTES = 32;

    private QuicTokenSecrets() {
    }

    public static byte[] resolve(Settings settings) {
        try (SecureString configured = HttpTransportSettings.SETTING_HTTP_QUIC_TOKEN_SECRET.get(settings)) {
            if (configured.isEmpty()) {
                return generateSecret();
            }
            return parse(configured.toString());
        }
    }

    public static byte[] parse(String value) {
        String trimmed = value.strip();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("http.quic.token_secret must not be empty");
        }
        byte[] secret;
        try {
            if (trimmed.regionMatches(true, 0, "hex:", 0, 4)) {
                secret = HexFormat.of().parseHex(trimmed.substring(4).strip());
            } else if (trimmed.regionMatches(true, 0, "base64:", 0, 7)) {
                secret = Base64.getDecoder().decode(trimmed.substring(7).strip());
            } else if (isHexString(trimmed)) {
                secret = HexFormat.of().parseHex(trimmed);
            } else {
                secret = Base64.getDecoder().decode(trimmed);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("http.quic.token_secret must be hex or base64 (optionally prefixed with hex: or base64:)", e);
        }
        if (secret.length < MIN_SECRET_BYTES) {
            throw new IllegalArgumentException(
                "http.quic.token_secret must decode to at least " + MIN_SECRET_BYTES + " bytes, but was "
                    + secret.length);
        }
        return secret;
    }

    public static byte[] generateSecret() {
        byte[] secret = new byte[GENERATED_SECRET_BYTES];
        new SecureRandom().nextBytes(secret);
        return secret;
    }

    private static boolean isHexString(String value) {
        if ((value.length() & 1) != 0) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (Character.digit(c, 16) < 0) {
                return false;
            }
        }
        return true;
    }
}
