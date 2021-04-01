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

package io.crate.protocols.ssl;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import io.crate.auth.AuthSettings;
import io.crate.common.Optionals;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

@Singleton
public class SslContextProvider {

    private static final Logger LOGGER = LogManager.getLogger(SslContextProvider.class);

    private volatile SslContext sslContext;

    private final Settings settings;

    @Inject
    public SslContextProvider(Settings settings) {
        this.settings = settings;
    }

    public SslContext getSslContext() {
        var localRef = sslContext;
        if (localRef == null) {
            synchronized (this) {
                localRef = sslContext;
                if (localRef == null) {
                    sslContext = localRef = buildSslContext();
                }
            }
        }
        return localRef;
    }

    public void reloadSslContext() {
        synchronized (this) {
            sslContext = buildSslContext();
            LOGGER.info("SSL configuration is reloaded.");
        }
    }

    private SslContext buildSslContext() {
        var keystorePath = SslConfigSettings.SSL_KEYSTORE_FILEPATH.setting().get(settings);
        var keystorePass = SslConfigSettings.SSL_KEYSTORE_PASSWORD.setting().get(settings).toCharArray();
        var keystoreKeyPass = SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD.setting().get(settings).toCharArray();

        var trustStorePath = SslConfigSettings.SSL_TRUSTSTORE_FILEPATH.setting().get(settings);
        var trustStorePass = SslConfigSettings.SSL_TRUSTSTORE_PASSWORD.setting().get(settings).toCharArray();

        try {
            var keyStore = loadKeyStore(keystorePath, keystorePass);
            var keyManagers = createKeyManagers(keyStore, keystoreKeyPass);

            var trustStore = Optionals.of(() -> loadKeyStore(trustStorePath, trustStorePass));
            var trustManagers = trustStore
                .map(SslContextProvider::createTrustManagers)
                .orElseGet(() -> new TrustManager[0]);

            // Use the newest SSL standard which is (at the time of writing) TLSv1.2
            // If we just specify "TLS" here, it depends on the JVM implementation which version we'll get.
            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(keyManagers, trustManagers, null);

            var keyStoreRootCerts = getRootCertificates(keyStore);
            var keyStoreCertChain = getCertificateChain(keyStore);
            var trustStoreRootCerts = trustStore
                .map(SslContextProvider::getRootCertificates)
                .orElseGet(() -> new X509Certificate[0]);
            PrivateKey privateKey = getPrivateKey(keyStore, keystoreKeyPass);
            return SslContextBuilder
                .forServer(privateKey, keyStoreCertChain)
                .ciphers(List.of(sslContext.createSSLEngine().getEnabledCipherSuites()))
                .applicationProtocolConfig(ApplicationProtocolConfig.DISABLED)
                .clientAuth(AuthSettings.resolveClientAuth(settings))
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

    private static TrustManager[] createTrustManagers(KeyStore keyStore) {
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

    static X509Certificate[] getRootCertificates(KeyStore keyStore) {
        ArrayList<X509Certificate> certs = new ArrayList<>();
        try {
            Enumeration<String> aliases = keyStore.aliases();
            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (keyStore.isCertificateEntry(alias)) {
                    var cert = (X509Certificate) keyStore.getCertificate(alias);
                    if (cert != null) {
                        certs.add(cert);
                    }
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
}
