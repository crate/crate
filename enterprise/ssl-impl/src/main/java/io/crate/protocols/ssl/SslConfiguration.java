/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.protocols.ssl;

import io.crate.common.Optionals;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import org.elasticsearch.common.settings.Settings;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
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

/**
 * Builds a Netty {@link SSLContext} which is passed upon creation of a {@link SslHandler}
 * which is responsible for establishing the SSL connection in a Netty pipeline.
 *
 * http://docs.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html
 *
 * TrustManager:
 * By convention, contains trusted certificates to verify the authenticity of an unknown
 * certificates which may be signed with a known certificate.
 * This is where your CA certificates go.
 *
 * KeyManager:
 * Contains the private key for data encryption and the certificate (public key) to send
 * to the remote end.
 *
 * See also {@link io.crate.protocols.postgres.SslReqHandler}
 */
@SuppressWarnings("WeakerAccess")
public final class SslConfiguration {

    private SslConfiguration() {}

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

    public static SslContext buildSslContext(Settings settings) {
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
                .map(SslConfiguration::createTrustManagers)
                .orElseGet(() -> new TrustManager[0]);

            // Use the newest SSL standard which is (at the time of writing) TLSv1.2
            // If we just specify "TLS" here, it depends on the JVM implementation which version we'll get.
            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(keyManagers, trustManagers, null);
            SSLContext.setDefault(sslContext);

            var keyStoreRootCerts = getRootCertificates(keyStore);
            var keyStoreCertChain = getCertificateChain(keyStore);
            var trustStoreRootCerts = trustStore
                .map(SslConfiguration::getRootCertificates)
                .orElseGet(() -> new X509Certificate[0]);
            PrivateKey privateKey = getPrivateKey(keyStore, keystoreKeyPass);
            return SslContextBuilder
                .forServer(privateKey, keyStoreCertChain)
                .ciphers(List.of(sslContext.createSSLEngine().getEnabledCipherSuites()))
                .applicationProtocolConfig(ApplicationProtocolConfig.DISABLED)
                .clientAuth(ClientAuth.OPTIONAL)
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
}
