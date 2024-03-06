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

package io.crate.protocols.ssl;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import io.crate.auth.AuthSettings;
import io.crate.auth.Protocol;
import io.crate.common.Optionals;
import org.jetbrains.annotations.VisibleForTesting;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

@Singleton
public class SslContextProvider implements Supplier<SslContext> {

    private static final String TLS_VERSION = "TLSv1.2";
    private static final Logger LOGGER = LogManager.getLogger(SslContextProvider.class);

    private final Map<Protocol, SslContext> sslContextPerProtocol = new ConcurrentHashMap<>();

    private final Settings settings;
    private final String keystorePath;
    private final char[] keystorePass;
    private final char[] keystoreKeyPass;
    private final String trustStorePath;
    private final char[] trustStorePass;



    @Inject
    public SslContextProvider(Settings settings) {
        this.settings = settings;
        this.keystorePath = SslSettings.SSL_KEYSTORE_FILEPATH.get(settings);
        this.keystorePass = SslSettings.SSL_KEYSTORE_PASSWORD.get(settings).toCharArray();
        this.keystoreKeyPass = SslSettings.SSL_KEYSTORE_KEY_PASSWORD.get(settings).toCharArray();

        this.trustStorePath = SslSettings.SSL_TRUSTSTORE_FILEPATH.get(settings);
        this.trustStorePass = SslSettings.SSL_TRUSTSTORE_PASSWORD.get(settings).toCharArray();
    }

    public SslContext getServerContext(Protocol protocol) {
        return sslContextPerProtocol.computeIfAbsent(protocol, p -> serverContext(p));
    }

    public void reloadSslContext() {
        synchronized (this) {
            sslContextPerProtocol.keySet().forEach(protocol -> sslContextPerProtocol.put(protocol, serverContext(protocol)));
            LOGGER.info("SSL configuration is reloaded.");
        }
    }

    @VisibleForTesting
    public SSLContext jdkSSLContext() throws Exception {
        var keyStore = loadKeyStore(keystorePath, keystorePass);
        var keyManagers = createKeyManagers(keyStore, keystoreKeyPass);

        var trustStore = Optionals.of(() -> loadKeyStore(trustStorePath, trustStorePass)).orElse(keyStore);
        var trustManagers = createTrustManagers(trustStore);

        SSLContext sslContext = SSLContext.getInstance(TLS_VERSION);
        sslContext.init(keyManagers, trustManagers, null);
        return sslContext;
    }

    public SslContext clientContext() {
        try {
            var keyStore = loadKeyStore(keystorePath, keystorePass);
            var keyManagers = createKeyManagers(keyStore, keystoreKeyPass);

            var trustStore = Optionals.of(() -> loadKeyStore(trustStorePath, trustStorePass));
            var trustManagers = trustStore
                .map(SslContextProvider::createTrustManagers)
                .orElseGet(() -> SslContextProvider.createTrustManagers(null));

            SSLContext sslContext = SSLContext.getInstance(TLS_VERSION);
            sslContext.init(keyManagers, trustManagers, null);

            var keyStoreRootCerts = getRootCertificates(keyStore);
            var trustStoreRootCerts = trustStore
                .map(SslContextProvider::getRootCertificates)
                .orElseGet(() -> SslContextProvider.getDefaultCertificates(trustManagers));
            return SslContextBuilder
                .forClient()
                .keyManager(getKey(keyStore, keystoreKeyPass), getCertificateChain(keyStore))
                .ciphers(List.of(sslContext.createSSLEngine().getEnabledCipherSuites()))
                .applicationProtocolConfig(ApplicationProtocolConfig.DISABLED)
                .trustManager(concat(keyStoreRootCerts, trustStoreRootCerts))
                .sessionCacheSize(0)
                .sessionTimeout(0)
                .startTls(false)
                .sslProvider(SslProvider.JDK)
                .build();
        } catch (SslConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new SslConfigurationException("Failed to build SSL configuration: " + e.getMessage(), e);
        }
    }

    @Nullable
    private PrivateKey getKey(KeyStore keyStore, char[] password) throws Exception {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            Key key = keyStore.getKey(alias, password);
            if (key instanceof PrivateKey privateKey) {
                return privateKey;
            }
        }
        return null;
    }

    private SslContext serverContext(Protocol protocol) {
        try {
            var keyStore = loadKeyStore(keystorePath, keystorePass);
            var keyManagers = createKeyManagers(keyStore, keystoreKeyPass);

            var trustStore = Optionals.of(() -> loadKeyStore(trustStorePath, trustStorePass));
            var trustManagers = trustStore
                .map(SslContextProvider::createTrustManagers)
                .orElseGet(() -> new TrustManager[0]);

            SSLContext sslContext = SSLContext.getInstance(TLS_VERSION);
            sslContext.init(keyManagers, trustManagers, null);

            var keyStoreCertChain = getCertificateChain(keyStore);
            var trustStoreRootCerts = trustStore
                .map(SslContextProvider::getRootCertificates)
                .orElseGet(() -> new X509Certificate[0]);
            PrivateKey privateKey = getPrivateKey(keyStore, keystoreKeyPass);
            return SslContextBuilder
                .forServer(privateKey, keyStoreCertChain)
                .ciphers(List.of(sslContext.createSSLEngine().getEnabledCipherSuites()))
                .applicationProtocolConfig(ApplicationProtocolConfig.DISABLED)
                .clientAuth(AuthSettings.resolveClientAuth(settings, protocol))
                .trustManager(concat(keyStoreCertChain, trustStoreRootCerts))
                .sessionCacheSize(0)
                .sessionTimeout(0)
                .startTls(false)
                .sslProvider(SslProvider.JDK)
                .build();
        } catch (SslConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new SslConfigurationException("Failed to build SSL configuration: " + e.getMessage(), e);
        }
    }

    static KeyStore loadKeyStore(String path, char[] pass) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (var stream = new BufferedInputStream(new FileInputStream(path))) {
            keyStore.load(stream, pass);
        }
        return keyStore;
    }

    static KeyManager[] createKeyManagers(KeyStore keyStore, char[] pass) throws Exception {
        var keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyFactory.init(keyStore, pass);
        return keyFactory.getKeyManagers();
    }

    static TrustManager[] createTrustManagers(@Nullable KeyStore keyStore) {
        // If trust store is not specified, default trust store will be used by calling init(null)
        TrustManagerFactory trustFactory;
        try {
            trustFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(keyStore);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return trustFactory.getTrustManagers();
    }

    private static <T> T[] concat(T[] xs, T[] ys) {
        T[] result = Arrays.copyOf(xs, xs.length + ys.length);
        System.arraycopy(ys, 0, result, xs.length, ys.length);
        return result;
    }

    static X509Certificate[] getDefaultCertificates(TrustManager[] trustManagers) {
        return Arrays.stream(trustManagers)
            .flatMap(tm -> Arrays.stream(((X509TrustManager) tm).getAcceptedIssuers()))
            .collect(Collectors.toList()).toArray(X509Certificate[]::new);
    }

    static X509Certificate[] getRootCertificates(KeyStore keyStore) {
        ArrayList<X509Certificate> certs = new ArrayList<>();
        try {
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                Certificate certificate = keyStore.getCertificate(alias);
                if (certificate instanceof X509Certificate x509Cert) {
                    certs.add(x509Cert);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return certs.toArray(new X509Certificate[0]);
    }

    static X509Certificate[] getCertificateChain(KeyStore keyStore) {
        ArrayList<X509Certificate> certs = new ArrayList<>();
        try {
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    Certificate[] certificateChain = keyStore.getCertificateChain(alias);
                    if (certificateChain != null) {
                        for (Certificate certificate : certificateChain) {
                            certs.add((X509Certificate) certificate);
                        }
                    }

                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return certs.toArray(new X509Certificate[0]);
    }

    static PrivateKey getPrivateKey(KeyStore keyStore, char[] password) throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                Key key = keyStore.getKey(alias, password);
                if (key instanceof PrivateKey) {
                    return (PrivateKey) key;
                }
            }
        }
        throw new KeyStoreException("No fitting private key found in keyStore");
    }

    @Override
    public SslContext get() {
        return getServerContext(Protocol.POSTGRES);
    }
}
