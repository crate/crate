/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.Strings;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.SigningKeyNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import io.netty.handler.codec.http.HttpHeaderNames;

/**
 * Custom @{@link JwkProvider} implementation based on
 * <a href="https://github.com/auth0/jwks-rsa-java/blob/master/src/main/java/com/auth0/jwk/UrlJwkProvider.java">UrlJwkProvider.java</a>
 * which caches results of public jwk keys for the duration of the "Cache-Control max-age"
 * Http header value from the response of the jwt authentication endpoint or
 * a provided default value.
 */
public class CachingJwkProvider implements JwkProvider {

    private final URL url;
    private final ObjectReader reader;
    private final Clock clock;
    private volatile JwkResult cache;

    public CachingJwkProvider(String domain) {
        this(domain, Clock.systemUTC());
    }

    CachingJwkProvider(String domain, Clock clock) {
        this.url = urlForDomain(domain);
        this.clock = clock;
        this.reader = new ObjectMapper().readerFor(Map.class);
    }

    static URL urlForDomain(String domain) {
        if (Strings.isNullOrEmpty(domain)) {
            throw new IllegalArgumentException("A domain is required");
        }
        if (!domain.startsWith("http")) {
            domain = "https://" + domain;
        }
        try {
            final URI uri = new URI(domain).normalize();
            return uri.toURL();
        } catch (MalformedURLException | URISyntaxException e) {
            throw new IllegalArgumentException("Invalid jwks uri", e);
        }
    }

    @Override
    public Jwk get(String keyId) throws JwkException {
        var keys = cache;
        Instant now = clock.instant();
        if (keys == null || keys.expired(now)) {
            keys = getKeys();
            if (!keys.expired(now)) {
                cache = keys;
            }
        }
        Jwk jwk = keys.keys().get(keyId);
        if (jwk == null) {
            throw new SigningKeyNotFoundException(
                "No key found in " + url.toString() + " with kid " + keyId,
                null
            );
        }
        return jwk;
    }

    @SuppressWarnings("unchecked")
    private JwkResult getKeys() {
        final List<Map<String, Object>> keys;
        final Duration ttl;
        try {
            final URLConnection c = this.url.openConnection();
            c.setRequestProperty("Accept", "application/json");
            try (InputStream inputStream = c.getInputStream()) {
                Map<String, Object> result = reader.readValue(inputStream);
                keys = (List<Map<String, Object>>) result.get("keys");

                String cacheControl = c.getHeaderField(HttpHeaderNames.CACHE_CONTROL.toString());
                ttl = parseCacheControl(cacheControl);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot obtain jwks from url " + url, e);
        }

        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("No keys found in " + url, null);
        }
        HashMap<String, Jwk> parsedKeys = HashMap.newHashMap(keys.size());
        for (Map<String, Object> key : keys) {
            Jwk jwk = Jwk.fromValues(key);
            parsedKeys.put(jwk.getId(), jwk);
        }
        return new JwkResult(clock.instant().plus(ttl), parsedKeys);
    }

    private record JwkResult(Instant expirationTime, Map<String, Jwk> keys) {

        public boolean expired(Instant now) {
            return now.compareTo(expirationTime) >= 0;
        }
    }

    @VisibleForTesting
    static Duration parseCacheControl(@Nullable String cacheControl) {
        if (cacheControl == null || !cacheControl.trim().startsWith("max-age=")) {
            return Duration.ZERO;
        }
        String maxAgeValue = cacheControl.substring(cacheControl.indexOf("=") + 1);
        try {
            int seconds = Integer.parseInt(maxAgeValue);
            return seconds > 0 ? Duration.ofSeconds(seconds) : Duration.ZERO;
        } catch (NumberFormatException ignored) {
            return Duration.ZERO;
        }
    }
}
