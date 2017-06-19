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

import io.crate.protocols.postgres.SslReqConfiguringHandler;
import io.crate.settings.CrateSetting;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.Optional;

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
 * See also {@link SslReqConfiguringHandler}
 */
public final class SslConfiguration {

    public static SslContext buildSslContext(Settings settings) {
        try {
            KeyStoreSettings keyStoreSettings = new KeyStoreSettings(settings);

            Optional<TrustStoreSettings> trustStoreSettings = TrustStoreSettings.tryLoad(settings);
            TrustManager[] trustManagers = null;
            if (trustStoreSettings.isPresent()) {
                trustManagers = trustStoreSettings.get().trustManagers;
            }

            // Use the newest SSL standard which is (at the time of writing) TLSv1.2
            // If we just specify "TLS" here, it depends on the JVM implementation which version we'll get.
            SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
            sslContext.init(keyStoreSettings.keyManagers, trustManagers, null);
            SSLContext.setDefault(sslContext);

            List<String> supportedCiphers = Arrays.asList(sslContext.createSSLEngine().getSupportedCipherSuites());

            final X509Certificate[] keystoreCerts = keyStoreSettings.exportServerCertChain();
            final PrivateKey privateKey = keyStoreSettings.exportDecryptedKey();

            X509Certificate[] trustedCertificates = keyStoreSettings.exportRootCertificates();
            if (trustStoreSettings.isPresent()) {
                trustedCertificates = trustStoreSettings.get().exportRootCertificates(trustedCertificates);
            }


            final SslContextBuilder sslContextBuilder =
                SslContextBuilder
                    .forServer(privateKey, keystoreCerts)
                    .ciphers(supportedCiphers)
                    .applicationProtocolConfig(ApplicationProtocolConfig.DISABLED)
                    .clientAuth(ClientAuth.OPTIONAL)
                    .sessionCacheSize(0)
                    .sessionTimeout(0)
                    .sslProvider(SslProvider.JDK);

            if (trustedCertificates != null && trustedCertificates.length > 0) {
                sslContextBuilder.trustManager(trustedCertificates);
            }

            return sslContextBuilder.build();

        } catch (SslConfigurationException e) {
            throw e;
        } catch (Exception e) {
            throw new SslConfigurationException("Failed to build SSL configuration", e);
        }
    }

    abstract static class AbstractKeyStoreSettings {

        final Logger LOGGER = Loggers.getLogger(getClass());

        final KeyStore keyStore;
        final String keyStorePath;
        final char[] keyStorePassword;

        AbstractKeyStoreSettings(Settings settings) throws Exception {
            this.keyStorePath = checkStorePath(getPathSetting().setting().get(settings));
            this.keyStorePassword = getPassword(getPasswordSetting().setting().get(settings));
            this.keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        }

        abstract CrateSetting<String> getPathSetting();

        abstract CrateSetting<String> getPasswordSetting();


        X509Certificate[] exportRootCertificates() throws KeyStoreException {
            return exportRootCertificates(new X509Certificate[0]);
        }

        X509Certificate[] exportRootCertificates(X509Certificate[] existingCerts) throws KeyStoreException {

            final List<X509Certificate> trustedCerts = new ArrayList<>();

            final Enumeration<String> aliases = keyStore.aliases();

            while (aliases.hasMoreElements()) {
                String _alias = aliases.nextElement();

                if (keyStore.isCertificateEntry(_alias)) {
                    LOGGER.info("Found certificate with alias {}", _alias);
                    final X509Certificate cert = (X509Certificate) keyStore.getCertificate(_alias);
                    if (cert != null) {
                        trustedCerts.add(cert);
                    }
                }
            }

            X509Certificate[] certsToAdd = trustedCerts.toArray(new X509Certificate[0]);

            X509Certificate[] trustedCertificates = Arrays.copyOf(
                existingCerts,
                existingCerts.length + certsToAdd.length);

            System.arraycopy(
                certsToAdd, 0,
                trustedCertificates, existingCerts.length,
                certsToAdd.length);

            return trustedCertificates;
        }

        X509Certificate[] exportServerCertChain() throws KeyStoreException {
            final Enumeration<String> aliases = keyStore.aliases();

            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    LOGGER.info("Found key with alias {}", alias);
                    Certificate[] certs = keyStore.getCertificateChain(alias);
                    if (certs != null && certs.length > 0) {
                        return Arrays.copyOf(certs, certs.length, X509Certificate[].class);
                    }
                }
            }

            return new X509Certificate[0];
        }

        static String checkStorePath(String keystoreFilePath) throws FileNotFoundException {

            if (keystoreFilePath == null || keystoreFilePath.length() == 0) {
                throw new FileNotFoundException("Empty file path for keystore.");
            }

            Path path = Paths.get(keystoreFilePath);
            if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                throw new FileNotFoundException("[" + keystoreFilePath + "] is a directory, expected file for keystore.");
            }

            if (!Files.isReadable(path)) {
                throw new FileNotFoundException("Unable to read [" + keystoreFilePath + "] for " +
                                                    "key store. Please make sure this file exists and has read permissions.");
            }

            return keystoreFilePath;
        }


        static char[] getPassword(String password) {
            if (password == null) {
                return new char[] {};
            }
            return password.toCharArray();
        }
    }

    static class KeyStoreSettings extends AbstractKeyStoreSettings {

        final KeyManager[] keyManagers;
        final char[] keyStoreKeyPassword;

        KeyStoreSettings(Settings settings) throws Exception {
            super(settings);

            this.keyStoreKeyPassword = getPassword(
                SslConfigSettings.SSL_KEYSTORE_KEY_PASSWORD.setting().get(settings));

            try (FileInputStream is = new FileInputStream(new File(keyStorePath))) {
                keyStore.load(is, keyStorePassword);
            }

            KeyManagerFactory keyFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyFactory.init(keyStore, keyStoreKeyPassword);

            this.keyManagers = keyFactory.getKeyManagers();
        }


        PrivateKey exportDecryptedKey()
                throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
            Enumeration<String> aliases = keyStore.aliases();
            if (!aliases.hasMoreElements()) {
                throw new KeyStoreException("No aliases found in keystore");
            }

            while (aliases.hasMoreElements()) {
                String alias = aliases.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    LOGGER.info("Found private key with alias {}", alias);
                    Key key = keyStore.getKey(alias, keyStoreKeyPassword);
                    if (key instanceof PrivateKey) {
                        return (PrivateKey) key;
                    }
                }
            }

            throw new KeyStoreException("No key matching the password found in keystore: " + keyStorePath);
        }

        @Override
        CrateSetting<String> getPathSetting() {
            return SslConfigSettings.SSL_KEYSTORE_FILEPATH;
        }

        @Override
        CrateSetting<String> getPasswordSetting() {
            return SslConfigSettings.SSL_KEYSTORE_PASSWORD;
        }

    }

    static class TrustStoreSettings extends AbstractKeyStoreSettings {

        final TrustManager[] trustManagers;

        private TrustStoreSettings(Settings settings) throws Exception {
            super(settings);

            keyStore.load(
                new FileInputStream(new File(keyStorePath)), keyStorePassword);

            TrustManagerFactory trustFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustFactory.init(keyStore);

            this.trustManagers = trustFactory.getTrustManagers();
        }

        @Override
        CrateSetting<String> getPathSetting() {
            return SslConfigSettings.SSL_TRUSTSTORE_FILEPATH;
        }

        @Override
        CrateSetting<String> getPasswordSetting() {
            return SslConfigSettings.SSL_TRUSTSTORE_PASSWORD;
        }

        static Optional<TrustStoreSettings> tryLoad(Settings settings) throws Exception {
            try {
                return Optional.of(new TrustStoreSettings(settings));
            } catch (FileNotFoundException e) {
                return Optional.empty();
            }
        }
    }
}
