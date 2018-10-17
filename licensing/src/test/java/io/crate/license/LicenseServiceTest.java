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
import org.elasticsearch.common.settings.Settings;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.crate.license.LicenseKey.VERSION;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class LicenseServiceTest extends CrateDummyClusterServiceUnitTest {

    private LicenseService licenseService;

    @Before
    public void setupLicenseService() {
        licenseService = new LicenseService(Settings.EMPTY, mock(TransportSetLicenseAction.class), clusterService);
    }

    @Test
    public void testVerifyValidLicense() {
        LicenseKey licenseKey = licenseService.createLicenseKey(LicenseKey.SELF_GENERATED, VERSION,
            new DecryptedLicenseData(Long.MAX_VALUE, "test"));
        assertThat(licenseService.verifyLicense(licenseKey), is(true));
    }

    @Test
    public void testVerifyExpiredLicense() {
        LicenseKey expiredLicense = licenseService.createLicenseKey(LicenseKey.SELF_GENERATED, VERSION,
            new DecryptedLicenseData(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(5), "test"));

        assertThat(licenseService.verifyLicense(expiredLicense), is(false));
    }

    @Test
    public void testGetLicenseData() throws IOException {
        LicenseKey licenseKey = licenseService.createLicenseKey(LicenseKey.SELF_GENERATED, VERSION,
            new DecryptedLicenseData(Long.MAX_VALUE, "test"));
        DecryptedLicenseData licenseData = licenseService.licenseData(LicenseKey.decodeLicense(licenseKey));

        assertThat(licenseData.expirationDateInMs(), is(Long.MAX_VALUE));
        assertThat(licenseData.issuedTo(), is("test"));
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
    public void testGetLicenseDataOnlySupportsSelfGeneratedLicense() throws IOException {
        DecodedLicense decodedLicense = new DecodedLicense(-2, VERSION, new byte[]{1, 2, 3, 4});

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Only self generated licenses are supported");
        licenseService.licenseData(decodedLicense);
    }

    @Test
    public void testOnlySelfGeneratedLicenseIsSupported() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Only self generated licenses are supported");

        licenseService.createLicenseKey(-2, VERSION, new DecryptedLicenseData(Long.MAX_VALUE, "test"));
    }
}
