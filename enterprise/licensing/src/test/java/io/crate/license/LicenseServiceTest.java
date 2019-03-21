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

package io.crate.license;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static io.crate.license.EnterpriseLicenseService.MAX_NODES_FOR_V1_LICENSES;
import static io.crate.license.EnterpriseLicenseService.UNLIMITED_EXPIRY_DATE_IN_MS;
import static io.crate.license.License.Type;
import static io.crate.license.LicenseConverterTest.createV1JsonLicense;
import static io.crate.license.LicenseKey.VERSION;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class LicenseServiceTest extends CrateDummyClusterServiceUnitTest {

    private EnterpriseLicenseService licenseService;

    private static byte[] getPrivateKey() {
        try (InputStream is = LicenseServiceTest.class.getResourceAsStream("/private.key")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static LicenseKey createEnterpriseLicenseKey(int version, LicenseData licenseData) {
        byte[] encryptedContent = encrypt(LicenseConverter.toJson(licenseData), getPrivateKey());
        return LicenseKey.encode(Type.ENTERPRISE,
            version,
            encryptedContent);
    }

    private static LicenseKey createV1EnterpriseLicenseKey(long expiryDateInMs, String issuedTo) {
        byte[] encryptedContent = encrypt(createV1JsonLicense(expiryDateInMs, issuedTo), getPrivateKey());
        return LicenseKey.encode(Type.ENTERPRISE,
            1,
            encryptedContent);
    }

    private static LicenseKey createV1TrialLicenseKey(long expiryDateInMs, String issuedTo) {
        return TrialLicense.createLicenseKey(1,
            createV1JsonLicense(expiryDateInMs, issuedTo)
        );
    }


    private static byte[] encrypt(byte[] data, byte[] privateKeyBytes) {
        return CryptoUtilsTest.encryptRsaUsingPrivateKey(data, privateKeyBytes);
    }

    @Before
    public void setupLicenseService() {
        licenseService = new EnterpriseLicenseService(
            mock(TransportService.class),
            mock(TransportSetLicenseAction.class));
    }

    @Test
    public void testGetLicenseDataForTrialLicenseKeyProduceValidValues() throws IOException {
        LicenseKey key = TrialLicense.createLicenseKey(VERSION,
            new LicenseData(Long.MAX_VALUE, "test", 3));
        LicenseData licenseData = LicenseKey.decode(key).licenseData();

        assertThat(licenseData.expiryDateInMs(), Matchers.is(Long.MAX_VALUE));
        assertThat(licenseData.issuedTo(), Matchers.is("test"));
        assertThat(licenseData.maxNumberOfNodes(), Matchers.is(3));
    }

    @Test
    public void testGetLicenseDataForTrialLicenseKeyV1ProduceValidValues() throws IOException {
        LicenseKey key = createV1TrialLicenseKey(Long.MAX_VALUE, "test");
        LicenseData licenseData = LicenseKey.decode(key).licenseData();

        assertThat(licenseData.expiryDateInMs(), Matchers.is(Long.MAX_VALUE));
        assertThat(licenseData.issuedTo(), Matchers.is("test"));
        assertThat(licenseData.maxNumberOfNodes(), Matchers.is(MAX_NODES_FOR_V1_LICENSES));
    }

    @Test
    public void testGetLicenseDataForEnterpriseLicenseKeyProduceValidValues() throws IOException {
        LicenseKey key = createEnterpriseLicenseKey(VERSION,
            new LicenseData(Long.MAX_VALUE, "test", 20));
        LicenseData licenseData = LicenseKey.decode(key).licenseData();

        assertThat(licenseData.expiryDateInMs(), Matchers.is(Long.MAX_VALUE));
        assertThat(licenseData.issuedTo(), Matchers.is("test"));
        assertThat(licenseData.maxNumberOfNodes(), Matchers.is(20));
    }

    @Test
    public void testGetLicenseDataForEnterpriseLicenseKeyV1ProduceValidValues() throws IOException {
        LicenseKey key = createV1EnterpriseLicenseKey(Long.MAX_VALUE, "test");
        LicenseData licenseData = LicenseKey.decode(key).licenseData();

        assertThat(licenseData.expiryDateInMs(), Matchers.is(Long.MAX_VALUE));
        assertThat(licenseData.issuedTo(), Matchers.is("test"));
        assertThat(licenseData.maxNumberOfNodes(), Matchers.is(MAX_NODES_FOR_V1_LICENSES));
    }

    @Test
    public void testVerifyValidTrialLicense() {
        LicenseKey licenseKey = TrialLicense.createLicenseKey(VERSION,
            new LicenseData(Long.MAX_VALUE, "test", 3));

        assertThat(EnterpriseLicenseService.verifyLicense(licenseKey), is(true));
    }

    @Test
    public void testVerifyExpiredTrialLicense() {
        LicenseKey expiredLicense = TrialLicense.createLicenseKey(VERSION,
            new LicenseData(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(5),
                "test",
                3)
        );

        assertThat(EnterpriseLicenseService.verifyLicense(expiredLicense), is(false));
    }

    @Test
    public void testVerifyValidEnterpriseLicense() {
        LicenseKey key = createEnterpriseLicenseKey(VERSION,
            new LicenseData(Long.MAX_VALUE, "test", 3));
        assertThat(EnterpriseLicenseService.verifyLicense(key), is(true));
    }

    @Test
    public void testLicenseNotificationIsExpiredForExpiredLicense() {
        LicenseData expiredLicense = new LicenseData(
            System.currentTimeMillis() - HOURS.toMillis(5), "test", 3);
        assertThat(LicenseExpiryNotification.of(expiredLicense), is(LicenseExpiryNotification.EXPIRED));
    }

    @Test
    public void testLicenseNotificationIsSevereForLicenseThatExpiresWithin1Day() {
        LicenseData licenseData = new LicenseData(
            System.currentTimeMillis() + HOURS.toMillis(5), "test", 3);
        assertThat(LicenseExpiryNotification.of(licenseData), is(LicenseExpiryNotification.SEVERE));
    }

    @Test
    public void testLicenseNotificationIsModerateForLicenseThatExpiresWithin15Days() {
        LicenseData licenseData = new LicenseData(
            System.currentTimeMillis() + DAYS.toMillis(13), "test", 3);
        assertThat(LicenseExpiryNotification.of(licenseData), is(LicenseExpiryNotification.MODERATE));
    }

    @Test
    public void testLicenseNotificationIsValidForLicenseWithMoreThan15DaysLeft() {
        LicenseData licenseData = new LicenseData(
            System.currentTimeMillis() + DAYS.toMillis(30), "test", 3);
        assertThat(LicenseExpiryNotification.of(licenseData), is(LicenseExpiryNotification.VALID));
    }

    @Test
    public void testVerifyTamperedEnterpriseLicenseThrowsException() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Decryption error");
        LicenseKey tamperedKey = new LicenseKey("AAAAAQAAAAEAAAEAU2d2c5fhWCGYEOlcedLoffev+ymwGM/yZKtMK50NYsTEFMz+Gun2YC7oRMy5rARb1gJbZQNRKBP/G2/e2QiS0ncvw/LChBLbTKD2m3PR1Efi9vl7GNgcz3pBbtDs/+/BDvTpOyyJxsvE9h+wUOnb9uppSK/kAx0/VIcXfezRSqFLOz7yH5F+w0rvXKYIuWZpfDpGmNl1gsBp6Pb+dPzA2rS8ty/+riaC5viT7gmdq+HJzAx28M1IYatq3IqwWpyG5HzciMSiiLVEAg7D1yj/QXoP39N3Ehceoh+Q9JCPndHDA0F54UZVGsMAddVkBO1kUf50sVXndwRJB9MUXVZQdQ==");
        EnterpriseLicenseService.verifyLicense(tamperedKey);
    }

    @Test
    public void testVerifyExpiredEnterpriseLicense() {
        LicenseKey key = createEnterpriseLicenseKey(VERSION,
            new LicenseData(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(5),
                "test",
                3)
        );

        assertThat(EnterpriseLicenseService.verifyLicense(key), is(false));
    }

    @Test
    public void testThatWhenRegisteringALicenseMaxNumberOfNodesIsNeverExceeded() {
        licenseService.onUpdatedLicense(ClusterState.EMPTY_STATE, null);
        assertThat(licenseService.isMaxNumberOfNodesExceeded(), is(false));
    }

    @Test
    public void testThatMaxNumberOfNodesIsWithinLimit() {
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder()
                .add(new DiscoveryNode("n1", buildNewFakeTransportAddress(),
                    Version.CURRENT))
                .add(new DiscoveryNode("n2", buildNewFakeTransportAddress(),
                    Version.CURRENT))
                .add(new DiscoveryNode("n3", buildNewFakeTransportAddress(),
                    Version.CURRENT))
                .localNodeId("n1")
            )
            .build();
        LicenseData licenseData = new LicenseData(UNLIMITED_EXPIRY_DATE_IN_MS, "test", 3);

        licenseService.onUpdatedLicense(state, licenseData);
        assertThat(licenseService.isMaxNumberOfNodesExceeded(), is(false));
    }

    @Test
    public void testThatMaxNumberOfNodesIsExceeded() {
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder()
                .add(new DiscoveryNode("n1", buildNewFakeTransportAddress(),
                    Version.CURRENT))
                .add(new DiscoveryNode("n2", buildNewFakeTransportAddress(),
                    Version.CURRENT))
                .add(new DiscoveryNode("n3", buildNewFakeTransportAddress(),
                    Version.CURRENT))
                .add(new DiscoveryNode("n4", buildNewFakeTransportAddress(),
                    Version.CURRENT))
                .localNodeId("n1")
            )
            .build();
        LicenseData licenseData = new LicenseData(UNLIMITED_EXPIRY_DATE_IN_MS, "test", 3);

        licenseService.onUpdatedLicense(state, licenseData);
        assertThat(licenseService.isMaxNumberOfNodesExceeded(), is(true));
    }
}
