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

import static io.crate.auth.AuthSettings.AUTH_HOST_BASED_JWT_ISS_SETTING;
import static io.crate.protocols.SSL.getSession;
import static io.netty.buffer.Unpooled.copiedBuffer;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.jspecify.annotations.Nullable;

import io.crate.auth.AuthSettings;
import io.crate.auth.Authentication;
import io.crate.auth.AuthenticationMethod;
import io.crate.auth.Credentials;
import io.crate.auth.PasswordAuthenticationMethod;
import io.crate.auth.Protocol;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.protocols.SSL;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.role.SecureHash;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;

/**
 * Authenticates a single HTTP request
 * <p>
 * Each request-handling handler ({@link io.crate.rest.action.SqlHttpHandler},
 * {@link HttpBlobHandler}, {@link MainAndStaticFileHandler}) must authenticate
 * the request using this class before creating a session.
 *
 * Must not be shared across threads, since cache and digest state are not thread safe.
 */
public final class HttpAuthenticator {

    private static final Logger LOGGER = LogManager.getLogger(HttpAuthenticator.class);

    @VisibleForTesting
    // realm-value should not contain any special characters
    static final String WWW_AUTHENTICATE_REALM_MESSAGE = "Basic realm=\"CrateDB Authenticator\"";

    private Mac mac;
    private static final String DIGEST_ALGORITHM = "HmacSHA256";

    private final Authentication authService;
    private final boolean checkJwtProperties;
    private final boolean supportXRealIp;
    private final String defaultUser;
    private final Roles roles;

    private final Map<String, CachedAuth> cache = new HashMap<>();

    private record CachedAuth(SecureHash secureHash, byte[] lastPassedCredentialsDigest) { }

    private final SecretKeySpec digestKey;

    public HttpAuthenticator(Settings settings, Authentication authService, Roles roles) {
        this.checkJwtProperties = settings.get(AUTH_HOST_BASED_JWT_ISS_SETTING.getKey()) == null;
        this.supportXRealIp = AuthSettings.AUTH_TRUST_HTTP_SUPPORT_X_REAL_IP.get(settings);
        this.defaultUser = AuthSettings.AUTH_TRUST_HTTP_DEFAULT_HEADER.get(settings);
        this.authService = authService;
        this.roles = roles;
        // HmacSHA256 outputs 256 bits, 32 bytes.
        // Taking 32 bytes as key length as per RFC recommendation.
        // From https://datatracker.ietf.org/doc/html/rfc4868:
        // key lengths less than the output length
        // decrease security strength, and keys longer than the output length do
        // not significantly increase security strength.
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        this.digestKey = new SecretKeySpec(key, DIGEST_ALGORITHM);
        try {
            mac = Mac.getInstance(DIGEST_ALGORITHM);
            mac.init(digestKey);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            mac = null;
            LOGGER.warn("Unable to initialize digest mac, won't be caching authenticated users", e);
        }
    }

    /**
     * On success the authenticated user is returned. On failure a {@code 401} response
     * is written to {@code channel}, the connection is closed and {@code null} is returned,
     * so the caller has to stop processing the request.
     */
    @Nullable
    public Role authenticate(HttpRequest request, Channel channel) {
        SSLSession session = getSession(channel);
        // The password object is released when the credentials are closed.
        try (Credentials credentials = credentialsFromRequest(request, session, defaultUser)) {
            Predicate<Role> rolePredicate = credentials.matchByToken(checkJwtProperties);
            if (rolePredicate != null) {
                Role role = roles.findUser(rolePredicate);
                if (role != null) {
                    credentials.setUsername(role.name());
                }
            }
            String username = credentials.username();
            InetAddress address = addressFromRequestOrChannel(request, channel);
            ConnectionProperties connectionProperties =
                new ConnectionProperties(credentials, address, Protocol.HTTP, session);

            AuthenticationMethod authMethod = authService.resolveAuthenticationType(username, connectionProperties);
            if (authMethod == null) {
                throw new RuntimeException(String.format(
                    Locale.ENGLISH,
                    "No valid auth.host_based.config entry found for host \"%s\", user \"%s\", protocol \"%s\". Did you enable TLS in your client?",
                    address.getHostAddress(), username, Protocol.HTTP));
            }

            byte[] credentialsDigest = null;
            if (mac != null && credentials.password() != null && PasswordAuthenticationMethod.NAME.equals(authMethod.name())) {
                credentialsDigest = digest(credentials.password().getChars());
                Role user = roles.findUser(username);
                if (user != null) {
                    Role cached = tryCacheLookup(user, credentialsDigest);
                    if (cached != null) {
                        return cached;
                    }
                }
            }

            Role user = authMethod.authenticate(credentials, connectionProperties);
            if (user == null) {
                throw new IllegalStateException(String.format(
                    Locale.ENGLISH,
                    "%s authentication didn't resolve a user for user \"%s\"",
                    authMethod.name(), username));
            }
            if (credentialsDigest != null) {
                cache.put(username, new CachedAuth(user.password(), credentialsDigest));
            }
            return user;
        } catch (Exception e) {
            sendUnauthorized(channel, e.getMessage());
            return null;
        }
    }

    @Nullable
    private Role tryCacheLookup(Role user, byte[] credentialsDigest) {
        CachedAuth cached = cache.get(user.name());
        if (cached == null || MessageDigest.isEqual(cached.lastPassedCredentialsDigest, credentialsDigest) == false) {
            return null;
        }

        // Password could be leaked and modified or reset (because of the leak).
        // If cache still has an old entry (real user hasn't re-authenticated yet after password change),
        // leaked password can pass lastPassedCredentialsDigest check,
        // so we need to check current password with the one user had when cache entry was created.
        var currentSecureHash = user.password();
        if (currentSecureHash == null) {
            return null;
        }
        if (currentSecureHash.equals(cached.secureHash)) {
            return user;
        }
        return null;
    }

    private byte[] digest(char[] chars) {
        for (int i = 0; i < chars.length; i++) {
            mac.update((byte) (chars[i] >> 8)); // high byte
            mac.update((byte) chars[i]); // low byte
        }
        // Resets state, safe to re-use for the next time.
        return mac.doFinal();
    }

    @VisibleForTesting
    static void sendUnauthorized(Channel channel, @Nullable String body) {
        HttpResponse response;
        if (body != null) {
            if (!body.endsWith("\n")) {
                body += "\n";
            }
            response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED, copiedBuffer(body, StandardCharsets.UTF_8));
            HttpUtil.setContentLength(response, body.length());
        } else {
            response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
        }
        // "Tell" the browser to open the credentials popup.
        // It helps to avoid custom login page in AdminUI.
        response.headers().set(HttpHeaderNames.WWW_AUTHENTICATE, WWW_AUTHENTICATE_REALM_MESSAGE);
        channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }

    @VisibleForTesting
    static Credentials credentialsFromRequest(HttpRequest request, @Nullable SSLSession session, String defaultUser) {
        String username = null;
        String authHeader = request.headers().get(HttpHeaderNames.AUTHORIZATION);
        if (authHeader != null) {
            // Prefer Http Auth (Basic or JWT, depending on header).
            return Headers.extractCredentialsFromHttpAuthHeader(authHeader);
        } else {
            // prefer commonName as userName over AUTH_TRUST_HTTP_DEFAULT_HEADER user
            if (session != null) {
                try {
                    Certificate certificate = session.getPeerCertificates()[0];
                    username = SSL.extractCN(certificate);
                } catch (ArrayIndexOutOfBoundsException | SSLPeerUnverifiedException ignored) {
                    // client cert is optional
                }
            }
            if (username == null) {
                username = defaultUser;
            }
        }
        return new Credentials(username, null);
    }

    private InetAddress addressFromRequestOrChannel(HttpRequest request, Channel channel) {
        if (supportXRealIp) {
            String realIPHeader = request.headers().get(AuthSettings.HTTP_HEADER_REAL_IP);
            if (realIPHeader == null) {
                return Netty4HttpServerTransport.getRemoteAddress(channel);
            }
            InetAddress realIP = InetAddresses.forString(realIPHeader);
            if (realIP.isLoopbackAddress() || realIP.isAnyLocalAddress() || realIP.isLinkLocalAddress()) {
                return Netty4HttpServerTransport.getRemoteAddress(channel);
            }
            return realIP;
        }
        return Netty4HttpServerTransport.getRemoteAddress(channel);
    }
}
