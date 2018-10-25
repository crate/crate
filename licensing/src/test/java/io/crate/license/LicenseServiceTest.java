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

package io.crate.license;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static io.crate.license.LicenseKey.VERSION;
import static io.crate.license.LicenseKey.LicenseType;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class LicenseServiceTest extends CrateDummyClusterServiceUnitTest {

    private LicenseService licenseService;

    private static byte[] getPrivateKey() {
        try (InputStream is = LicenseServiceTest.class.getResourceAsStream("/private.key")) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static LicenseKey createEnterpriseLicenseKey(int version, DecryptedLicenseData decryptedLicenseData) {
        byte[] encryptedContent = encrypt(decryptedLicenseData.formatLicenseData(), getPrivateKey());
        return LicenseKey.createLicenseKey(LicenseType.ENTERPRISE,
            version,
            encryptedContent);
    }

    private static byte[] encrypt(byte[] data, byte[] privateKeyBytes) {
        return CryptoUtilsTest.encryptRsaUsingPrivateKey(data, privateKeyBytes);
    }

    @Before
    public void setupLicenseService() {
        licenseService = new LicenseService(Settings.EMPTY, mock(TransportSetLicenseAction.class), clusterService);
    }

    @Test
    public void testGetLicenseDataForTrialLicenseKeyProduceValidValues() throws IOException {
        LicenseKey key = TrialLicense.createLicenseKey(VERSION, new DecryptedLicenseData(Long.MAX_VALUE, "test"));
        DecryptedLicenseData licenseData = LicenseService.licenseData(LicenseKey.decodeLicense(key));

        assertThat(licenseData.expirationDateInMs(), Matchers.is(Long.MAX_VALUE));
        assertThat(licenseData.issuedTo(), Matchers.is("test"));
    }

    @Test
    public void testGetLicenseDataForEnterpriseLicenseKeyProduceValidValues() throws IOException {
        LicenseKey key = createEnterpriseLicenseKey(VERSION, new DecryptedLicenseData(Long.MAX_VALUE, "test"));
        DecryptedLicenseData licenseData = LicenseService.licenseData(LicenseKey.decodeLicense(key));

        assertThat(licenseData.expirationDateInMs(), Matchers.is(Long.MAX_VALUE));
        assertThat(licenseData.issuedTo(), Matchers.is("test"));
    }

    @Test
    public void testVerifyValidTrialLicense() {
        LicenseKey licenseKey = TrialLicense.createLicenseKey(VERSION,
            new DecryptedLicenseData(Long.MAX_VALUE, "test"));

        assertThat(LicenseService.verifyLicense(licenseKey), is(true));
    }

    @Test
    public void testVerifyExpiredTrialLicense() {
        LicenseKey expiredLicense = TrialLicense.createLicenseKey(VERSION,
            new DecryptedLicenseData(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(5),"test"));

        assertThat(LicenseService.verifyLicense(expiredLicense), is(false));
    }

    @Test
    public void testVerifyValidEnterpriseLicense() {
        LicenseKey key = createEnterpriseLicenseKey(VERSION,
            new DecryptedLicenseData(Long.MAX_VALUE, "test"));
        assertThat(LicenseService.verifyLicense(key), is(true));
    }

    @Test
    public void testLicenseNotificationIsExpiredForExpiredLicense() {
        DecryptedLicenseData expiredLicense = new DecryptedLicenseData(
            System.currentTimeMillis() - HOURS.toMillis(5), "test");
        assertThat(licenseService.getLicenseExpiryNotification(expiredLicense), is(LicenseExpiryNotification.EXPIRED));
    }

    @Test
    public void testLicenseNotificationIsSevereForLicenseThatExpiresWithin1Day() {
        DecryptedLicenseData licenseData = new DecryptedLicenseData(
            System.currentTimeMillis() + HOURS.toMillis(5), "test");
        assertThat(licenseService.getLicenseExpiryNotification(licenseData), is(LicenseExpiryNotification.SEVERE));
    }

    @Test
    public void testLicenseNotificationIsModerateForLicenseThatExpiresWithin15Days() {
        DecryptedLicenseData licenseData = new DecryptedLicenseData(
            System.currentTimeMillis() + DAYS.toMillis(13), "test");
        assertThat(licenseService.getLicenseExpiryNotification(licenseData), is(LicenseExpiryNotification.MODERATE));
    }

    @Test
    public void testLicenseNotificationIsNullForLicenseWithMoreThan15DaysLeft() {
        DecryptedLicenseData licenseData = new DecryptedLicenseData(
            System.currentTimeMillis() + DAYS.toMillis(30), "test");
        assertThat(licenseService.getLicenseExpiryNotification(licenseData), is(Matchers.nullValue()));
    }

    @Test
    public void testVerifyTamperedEnterpriseLicenseThrowsException() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Decryption error");
        LicenseKey tamperedKey = new LicenseKey("AAAAAQAAAAEAAAEAU2d2c5fhWCGYEOlcedLoffev+ymwGM/yZKtMK50NYsTEFMz+Gun2YC7oRMy5rARb1gJbZQNRKBP/G2/e2QiS0ncvw/LChBLbTKD2m3PR1Efi9vl7GNgcz3pBbtDs/+/BDvTpOyyJxsvE9h+wUOnb9uppSK/kAx0/VIcXfezRSqFLOz7yH5F+w0rvXKYIuWZpfDpGmNl1gsBp6Pb+dPzA2rS8ty/+riaC5viT7gmdq+HJzAx28M1IYatq3IqwWpyG5HzciMSiiLVEAg7D1yj/QXoP39N3Ehceoh+Q9JCPndHDA0F54UZVGsMAddVkBO1kUf50sVXndwRJB9MUXVZQdQ==");
        LicenseService.verifyLicense(tamperedKey);
    }

    @Test
    public void testVerifyExpiredEnterpriseLicense() {
        LicenseKey key = createEnterpriseLicenseKey(VERSION,
            new DecryptedLicenseData(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(5), "test"));

        assertThat(LicenseService.verifyLicense(key), is(false));
    }
}
