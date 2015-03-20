/*
Copyright 2015 Hendrik Saly

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package io.crate.security.util;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.crypto.Cipher;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

public class SecurityUtil {

    private static final ESLogger log = Loggers.getLogger(SecurityUtil.class);
    private static final String[] PREFERRED_SSL_CIPHERS = { "TLS_RSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", "TLS_DHE_RSA_WITH_AES_256_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384", "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256" };
    private static final String[] PREFERRED_SSL_PROTOCOLS = { "TLSv1", "TLSv1.1", "TLSv1.2" };

    public static String[] ENABLED_SSL_PROTOCOLS = null;
    public static String[] ENABLED_SSL_CIPHERS = null;

    private SecurityUtil() {

    }

    static {
        try {
            final int aesMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES");

            if (aesMaxKeyLength < 256) {
                log.warn("AES 256 not supported, max key length for AES is " + aesMaxKeyLength
                        + ". To enable AES 256 install 'Java Cryptography Extension (JCE) Unlimited Strength Jurisdiction Policy Files'");
            }
        } catch (final NoSuchAlgorithmException e) {
            log.error("AES encryption not supported. " + e);

        }

        try {

            final SSLContext serverContext = SSLContext.getInstance("TLS");
            serverContext.init(null, null, null);
            final SSLEngine engine = serverContext.createSSLEngine();
            final List<String> supportedCipherSuites = new ArrayList<String>(Arrays.asList(engine.getSupportedCipherSuites()));
            final List<String> supportedProtocols = new ArrayList<String>(Arrays.asList(engine.getSupportedProtocols()));

            final List<String> preferredCipherSuites = Arrays.asList(PREFERRED_SSL_CIPHERS);
            final List<String> preferredProtocols = Arrays.asList(PREFERRED_SSL_PROTOCOLS);

            supportedCipherSuites.retainAll(preferredCipherSuites);
            supportedProtocols.retainAll(preferredProtocols);

            if (supportedCipherSuites.isEmpty()) {
                log.error("No usable SSL/TLS cipher suites found");
            } else {
                ENABLED_SSL_CIPHERS = supportedCipherSuites.toArray(new String[supportedCipherSuites.size()]);
            }

            if (supportedProtocols.isEmpty()) {
                log.error("No usable SSL/TLS protocols found");
            } else {
                ENABLED_SSL_PROTOCOLS = supportedProtocols.toArray(new String[supportedProtocols.size()]);
            }

            log.debug("Usable SSL/TLS protocols: {}", supportedProtocols);
            log.debug("Usable SSL/TLS cipher suites: {}", supportedCipherSuites);

        } catch (final Exception e) {
            log.error("Error while evaluating supported crypto", e);
        }
    }

    public static File getAbsoluteFilePathFromClassPath(final String fileNameFromClasspath) {

        File jaasConfigFile = null;
        final URL jaasConfigURL = SecurityUtil.class.getClassLoader().getResource(fileNameFromClasspath);
        if (jaasConfigURL != null) {
            try {
                jaasConfigFile = new File(URLDecoder.decode(jaasConfigURL.getFile(), "UTF-8"));
            } catch (final UnsupportedEncodingException e) {
                return null;
            }

            if (jaasConfigFile.exists() && jaasConfigFile.canRead()) {
                return jaasConfigFile;
            } else {
                log.error("Cannot read from {}, maybe the file does not exists? ", jaasConfigFile.getAbsolutePath());
            }

        } else {
            log.error("Failed to load " + fileNameFromClasspath);
        }

        return null;

    }

    public static boolean setSystemPropertyToAbsoluteFilePathFromClassPath(final String property, final String fileNameFromClasspath) {
        if (System.getProperty(property) == null) {
            File jaasConfigFile = null;
            final URL jaasConfigURL = SecurityUtil.class.getClassLoader().getResource(fileNameFromClasspath);
            if (jaasConfigURL != null) {
                try {
                    jaasConfigFile = new File(URLDecoder.decode(jaasConfigURL.getFile(), "UTF-8"));
                } catch (final UnsupportedEncodingException e) {
                    return false;
                }

                if (jaasConfigFile.exists() && jaasConfigFile.canRead()) {
                    System.setProperty(property, jaasConfigFile.getAbsolutePath());

                    log.debug("Load " + fileNameFromClasspath + " from {} ", jaasConfigFile.getAbsolutePath());
                    return true;
                } else {
                    log.error("Cannot read from {}, maybe the file does not exists? ", jaasConfigFile.getAbsolutePath());
                }

            } else {
                log.error("Failed to load " + fileNameFromClasspath);
            }
        } else {
            log.warn("Property " + property + " already set to " + System.getProperty(property));
        }

        return false;
    }

    public static boolean setSystemPropertyToAbsoluteFile(final String property, final String fileName) {
        if (System.getProperty(property) == null) {

            if (fileName == null) {
                log.error("Cannot set property " + property + " because filename is null");

                return false;
            }

            final File jaasConfigFile = new File(fileName).getAbsoluteFile();

            if (jaasConfigFile.exists() && jaasConfigFile.canRead()) {
                System.setProperty(property, jaasConfigFile.getAbsolutePath());

                log.debug("Load " + fileName + " from {} ", jaasConfigFile.getAbsolutePath());
                return true;
            } else {
                log.error("Cannot read from {}, maybe the file does not exists? ", jaasConfigFile.getAbsolutePath());
            }

        } else {
            log.warn("Property " + property + " already set to " + System.getProperty(property));
        }

        return false;
    }
}
