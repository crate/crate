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

import static io.crate.testing.auth.RsaKeys.PRIVATE_KEY_256;
import static io.crate.testing.auth.RsaKeys.PUBLIC_KEY_256;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.security.KeyFactory;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonGenerator;

import io.crate.http.HttpTestServer;
import io.crate.testing.UseJdbc;
import io.netty.handler.codec.http.HttpHeaderNames;

@UseJdbc(value = 0) // jwt is supported only for http
public class JwtAuthenticationIntegrationTest extends IntegTestCase {

    private static final Base64.Decoder BASE_64_DECODER = Base64.getDecoder();

    private static final Base64.Encoder BASE_64_URL_ENCODER = Base64.getUrlEncoder();
    private static final String KID = "1";

    /**
     * Imitates a JWK endpoint response with pre-generated public key.
     * Public key format is aligned with
     * https://console.cratedb-dev.cloud/api/v2/meta/jwk/
     * https://login.microsoftonline.com/common/discovery/v2.0/keys
     * https://www.googleapis.com/oauth2/v3/certs
     * and looks like:
     * {
     *   "keys":[
     *     {
     *       "e":"...",
     *       "kid":"...",
     *       "kty":"RSA",
     *        "n":"..."
     *     }
     *    ]
     * }
     */

    private final class RequestHandler implements BiConsumer<io.netty.handler.codec.http.HttpRequest, JsonGenerator> {

        AtomicInteger numberOfInvocation = new AtomicInteger(0);

        @Override
        public void accept(io.netty.handler.codec.http.HttpRequest httpRequest, com.fasterxml.jackson.core.JsonGenerator generator) {
            try {
                numberOfInvocation.incrementAndGet();
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(BASE_64_DECODER.decode(PUBLIC_KEY_256));
                RSAPublicKey publicKey = (RSAPublicKey) keyFactory.generatePublic(publicKeySpec);

                generator.writeStartObject();
                generator.writeArrayFieldStart("keys");
                generator.writeStartObject();
                generator.writeStringField("e", BASE_64_URL_ENCODER.encodeToString(publicKey.getPublicExponent().toByteArray()));
                generator.writeStringField("kid", KID);
                generator.writeStringField("kty", "RSA");
                generator.writeStringField("n", BASE_64_URL_ENCODER.encodeToString(publicKey.getModulus().toByteArray()));
                generator.writeEndObject();
                generator.writeEndArray();
                generator.writeEndObject();
                generator.close();
            } catch (Exception e) {
                throw new RuntimeException(e.getCause());
            }
        }
    }

    private HttpTestServer testServer;
    private RSAPrivateKey privateKey;

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("auth.host_based.enabled", true)
            .put("auth.host_based.config",
                "a", new String[]{"user", "method", "protocol"}, new String[]{"John", "jwt", "http"})
            .put("auth.host_based.config",
                "b", new String[]{"user", "method"}, new String[]{"John", "password"})
            .put("auth.host_based.config",
                "c", new String[]{"user", "method", "protocol"}, new String[]{"Joe", "jwt", "http"})
            .put("auth.host_based.config",
                "d", new String[]{"user", "method"}, new String[]{"Joe", "password"})
            .build();
    }

    @Before
    public void init() throws Exception {
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        // Prepare private key.
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(BASE_64_DECODER.decode(PRIVATE_KEY_256));
        privateKey = (RSAPrivateKey) keyFactory.generatePrivate(privateKeySpec);
    }

    @After
    public void cleanUp() {
        execute("DROP USER IF EXISTS \"John\"");
        execute("DROP USER IF EXISTS \"Joe\"");
        if (testServer != null) {
            testServer.shutDown();
        }
    }

    HttpResponse<String> sendRequest(URI uri, String jwt) throws IOException, InterruptedException {
        try (var client = HttpClient.newHttpClient()) {
            HttpRequest request = HttpRequest.newBuilder(uri)
                .header(HttpHeaderNames.AUTHORIZATION.toString(), "Bearer " + jwt)
                .header(HttpHeaderNames.ORIGIN.toString(), "http://example.com")
                .header(HttpHeaderNames.ACCESS_CONTROL_REQUEST_METHOD.toString(), "GET")
                .build();
            return client.send(request, BodyHandlers.ofString());
        }
    }

    String jwt(String appUsername, String iss, RSAPrivateKey privateKey) {
        // We use random port for the test suite (assigned by kernel)
        // Port affects url --> affects signature --> need to re-compute payload.
        return JWT.create()
            .withHeader(Map.of("typ", "JWT", "alg", "RS256", "kid", KID))
            .withIssuer(iss)
            .withAudience(clusterService().state().metadata().clusterUUID())
            .withClaim("username", appUsername)
            .sign(Algorithm.RSA256(null, privateKey));
    }

    @Test
    public void test_can_authenticate_with_jwt_token() throws Exception {
        testServer = new HttpTestServer(0, false, new RequestHandler(), Map.of(HttpHeaderNames.CACHE_CONTROL.toString(), "max-age=1000"));
        testServer.run();

        String appUsername = "cloud_user";
        String iss = String.format(Locale.ENGLISH, "http://localhost:%d/keys",testServer.boundPort());
        String jwt = jwt(appUsername, iss, privateKey);

        // Important to surround name with quotes if name used in HBA is not in lowercase
        // Otherwise CREATE USER saves it in lowercase whereas HBA entry was created for "John"
        execute("CREATE USER \"John\" " +
            "WITH (jwt = {\"iss\" = '" + iss + "', \"username\" = '" + appUsername + "'})"
        );

        HttpServerTransport httpTransport = cluster().getInstance(HttpServerTransport.class);
        InetSocketAddress address = httpTransport.boundAddress().publishAddress().address();
        URI uri = URI.create(String.format(Locale.ENGLISH, "http://%s:%s/", address.getHostName(), address.getPort()));

        var resp = sendRequest(uri, jwt);
        assertThat(resp.body()).containsIgnoringWhitespaces("""
            {
              "ok" : true,
              "status" : 200
            """);
    }


    @Test
    public void test_can_authenticate_with_jwt_token_cached() throws Exception {
        RequestHandler requestHandler = new RequestHandler();
        testServer = new HttpTestServer(0, false, requestHandler, Map.of(HttpHeaderNames.CACHE_CONTROL.toString(), "max-age=1000"));
        testServer.run();

        String appUsername = "cloud_user";
        String iss = String.format(Locale.ENGLISH, "http://localhost:%d/keys",testServer.boundPort());
        String jwt = jwt(appUsername, iss, privateKey);

        // Important to surround name with quotes if name used in HBA is not in lowercase
        // Otherwise CREATE USER saves it in lowercase whereas HBA entry was created for "John"
        execute("CREATE USER \"John\" " +
            "WITH (jwt = {\"iss\" = '" + iss + "', \"username\" = '" + appUsername + "'})"
        );

        HttpServerTransport httpTransport = cluster().getInstance(HttpServerTransport.class);
        InetSocketAddress address = httpTransport.boundAddress().publishAddress().address();
        URI uri = URI.create(String.format(Locale.ENGLISH, "http://%s:%s/", address.getHostName(), address.getPort()));

        var resp = sendRequest(uri, jwt);
        assertThat(resp.body()).containsIgnoringWhitespaces("""
                {
                  "ok" : true,
                  "status" : 200
                  """);

        assertThat(requestHandler.numberOfInvocation.get()).isEqualTo(1);

        // Now try a second time, same domain different user
        appUsername = "cloud_user_1";
        jwt = jwt(appUsername, iss, privateKey);

        execute("CREATE USER \"Joe\" " +
            "WITH (jwt = {\"iss\" = '" + iss + "', \"username\" = '" + appUsername + "'})"
        );

        resp = sendRequest(uri, jwt);
        assertThat(resp.body()).containsIgnoringWhitespaces("""
                {
                  "ok" : true,
                  "status" : 200
                  """);

        assertThat(requestHandler.numberOfInvocation.get()).isEqualTo(1);

    }

    @Test
    public void test_body_jwk_endpoint_not_responding_contains_error() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        String iss = String.format(Locale.ENGLISH, "http://localhost:%d/keys", port);
        String appUsername = "cloud_user";
        String jwt = jwt(appUsername, iss, privateKey);

        execute("CREATE USER \"John\" " +
            "WITH (jwt = {\"iss\" = '" + iss + "', \"username\" = '" + appUsername + "'})"
        );

        HttpServerTransport httpTransport = cluster().getInstance(HttpServerTransport.class);
        InetSocketAddress address = httpTransport.boundAddress().publishAddress().address();
        URI uri = URI.create(String.format(Locale.ENGLISH, "http://%s:%s/", address.getHostName(), address.getPort()));
        var resp = sendRequest(uri, jwt);
        assertThat(resp.body()).contains("jwt authentication failed for user John. Reason: Cannot obtain jwks from url");

    }
}
