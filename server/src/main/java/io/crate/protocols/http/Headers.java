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

import io.crate.auth.Credentials;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpVersion;

import org.jetbrains.annotations.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.regex.Pattern;

public final class Headers {

    private static final Pattern USER_AGENT_BROWSER_PATTERN = Pattern.compile("(Mozilla|Chrome|Safari|Opera|Android|AppleWebKit)+?[/\\s][\\d.]+");

    static boolean isBrowser(@Nullable String headerValue) {
        if (headerValue == null) {
            return false;
        }
        String engine = headerValue.split("\\s+")[0];
        return USER_AGENT_BROWSER_PATTERN.matcher(engine).matches();
    }

    static boolean isAcceptJson(String headerValue) {
        return headerValue != null && headerValue.contains("application/json");
    }

    public static boolean isCloseConnection(FullHttpRequest request) {
        HttpHeaders headers = request.headers();
        return HttpHeaderValues.CLOSE.contentEqualsIgnoreCase(headers.get(HttpHeaderNames.CONNECTION))
               || (request.protocolVersion().equals(HttpVersion.HTTP_1_0)
                   && !HttpHeaderValues.KEEP_ALIVE.contentEqualsIgnoreCase(headers.get(HttpHeaderNames.CONNECTION)));
    }

    public static void setKeepAlive(HttpVersion httpVersion, FullHttpResponse resp) {
        if (httpVersion.equals(HttpVersion.HTTP_1_0)) {
            resp.headers().add(HttpHeaderNames.CONNECTION, "Keep-Alive");
        }
    }

    /**
     * An entry point for HTTP authentication, forwards to either Basic or JWT, depending on header.
     * @param authHeaderValue contains authentication schema (basic or bearer) and auth payload (token or password) separated by space.
     * Authentication schema is case-insensitive: <a href="https://datatracker.ietf.org/doc/html/rfc7235?ref=blog.teknkl.com#section-2.1">spec</a>
     */
    public static Credentials extractCredentialsFromHttpAuthHeader(String authHeaderValue) {
        if (authHeaderValue == null || authHeaderValue.isEmpty()) {
            // Empty credentials.
            return new Credentials(null, null);
        }
        String[] splitHeader = authHeaderValue.split(" ");
        assert splitHeader.length == 2 :
            "Header must contain only authentication schema and base64 encoded value, separated by a whitespace";
        return switch (splitHeader[0].toLowerCase(Locale.ENGLISH)) {
            case "basic" -> extractCredentialsFromHttpBasicAuthHeader(splitHeader[1]);
            case "bearer" -> new Credentials(splitHeader[1]);
            default ->
                    throw new IllegalArgumentException("Only basic or bearer HTTP Authentication schemas are allowed.");
        };
    }

    private static Credentials extractCredentialsFromHttpBasicAuthHeader(String valueWithoutBasePrefix) {
        String username;
        char[] password = null;
        String decodedCreds = new String(Base64.getDecoder().decode(valueWithoutBasePrefix), StandardCharsets.UTF_8);

        int idx = decodedCreds.indexOf(':');
        if (idx < 0) {
            username = decodedCreds;
        } else {
            username = decodedCreds.substring(0, idx);
            String passwdStr = decodedCreds.substring(idx + 1);
            if (!passwdStr.isEmpty()) {
                password = passwdStr.toCharArray();
            }
        }
        return new Credentials(username, password);
    }
}
