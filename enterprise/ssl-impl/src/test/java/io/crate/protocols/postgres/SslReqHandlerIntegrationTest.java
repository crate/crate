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

package io.crate.protocols.postgres;

import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.protocols.ssl.SslConfigSettings;
import io.crate.testing.SQLResponse;
import io.crate.testing.UseJdbc;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.TimeUnit;


@UseJdbc(value = 1)
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SslReqHandlerIntegrationTest extends SQLTransportIntegrationTest {

    private final static char[] EMPTY_PASS = new char[]{};

    private static SelfSignedCertificate trustedCert;
    private static File keyStoreFile;

    public SslReqHandlerIntegrationTest() {
        super(true);
    }

    @BeforeClass
    public static void beforeTest() throws Exception {
        trustedCert = new SelfSignedCertificate();
        keyStoreFile = createTempFile().toFile();

        updateKeyStore(
            keyStoreFile,
            store -> {
                SelfSignedCertificate fakeCert = new SelfSignedCertificate();
                store.setKeyEntry(
                    "key",
                    trustedCert.key(),
                    EMPTY_PASS,
                    new Certificate[]{fakeCert.cert()});
            });
    }

    private static void updateKeyStore(File keyStoreFile, CheckedConsumer<KeyStore, Exception> consumer)
        throws Exception {
        var keyStore = KeyStore.getInstance("JKS");
        try (var is = new FileInputStream(keyStoreFile)) {
            keyStore.load(is, EMPTY_PASS);
        } catch (Exception e) {
            keyStore.load(null);
        }
        consumer.accept(keyStore);
        keyStore.store(new FileOutputStream(keyStoreFile), EMPTY_PASS);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(SslConfigSettings.SSL_PSQL_ENABLED.getKey(), true)
            .put(SslConfigSettings.SSL_KEYSTORE_FILEPATH.getKey(), keyStoreFile.getAbsolutePath())
            .put(SslConfigSettings.SSL_RESOURCE_POLL_INTERVAL.getKey(), "2s")
            .build();
    }

    @Test
    public void testReloadSslContextOnKeyStoreChange() throws Throwable {
        try {
            execute("select name from sys.nodes");
            fail("Unknown certificate in the certificate chain");
        } catch (Exception ignored) {
        }

        updateKeyStore(
            keyStoreFile,
            keyStore -> keyStore.setKeyEntry(
                "key",
                trustedCert.key(),
                EMPTY_PASS,
                new Certificate[]{trustedCert.cert()}));

        assertBusy(() -> {
            try {
                SQLResponse response = execute("select name from sys.nodes");
                assertEquals(1, response.rowCount());
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }, 20, TimeUnit.SECONDS);
    }
}
