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

package io.crate.expression.reference.sys.check.cluster;

import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.license.DecryptedLicenseData;
import io.crate.license.LicenseExpiryNotification;
import io.crate.license.LicenseService;
import io.crate.settings.SharedSettings;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LicenseExpiryCheckTest extends CrateUnitTest {

    private LicenseService licenseService;
    private LicenseExpiryCheck expirationCheck;

    @Before
    public void setupLicenseCheck() {
        licenseService = mock(LicenseService.class);
        Settings settings = Settings.builder().put("license.enterprise", true).build();
        expirationCheck = new LicenseExpiryCheck(settings, licenseService);
    }

    @After
    public void tearDown() throws Exception {
        assertSettingDeprecationsAndWarnings(new Setting<?>[] {SharedSettings.ENTERPRISE_LICENSE_SETTING.setting()});
        super.tearDown();
    }

    @Test
    public void testSysCheckMetadata() {
        assertThat(expirationCheck.id(), is(6));
    }

    @Test
    public void testValidLicense() {
        DecryptedLicenseData thirtyDaysLicense = new DecryptedLicenseData(
            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(30), "test");
        when(licenseService.currentLicense()).thenReturn(thirtyDaysLicense);
        when(licenseService.getLicenseExpiryNotification(thirtyDaysLicense)).thenReturn(null);
        assertThat(expirationCheck.validate(), is(true));
    }

    @Test
    public void testLessThanFifteenDaysToExpiryTriggersMediumCheck() {
        DecryptedLicenseData sevenDaysLicense = new DecryptedLicenseData(
            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(7), "test");

        when(licenseService.currentLicense()).thenReturn(sevenDaysLicense);
        when(licenseService.getLicenseExpiryNotification(sevenDaysLicense)).thenReturn(LicenseExpiryNotification.MODERATE);
        assertThat(expirationCheck.validate(), is(false));
        assertThat(expirationCheck.severity(), is(SysCheck.Severity.MEDIUM));
    }

    @Test
    public void testLessThanOneDayToExpiryTriggersSevereCheck() {
        DecryptedLicenseData sevenDaysLicense = new DecryptedLicenseData(
            System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15), "test");

        when(licenseService.currentLicense()).thenReturn(sevenDaysLicense);
        when(licenseService.getLicenseExpiryNotification(sevenDaysLicense)).thenReturn(LicenseExpiryNotification.SEVERE);
        assertThat(expirationCheck.validate(), is(false));
        assertThat(expirationCheck.severity(), is(SysCheck.Severity.HIGH));
    }

    @Test
    public void testCheckIsAlwaysValidWhenEnterpriseIsDisabled() {
        Settings settings = Settings.builder().put("license.enterprise", false).build();
        LicenseExpiryCheck expiryCheckNoEnterprise = new LicenseExpiryCheck(settings, mock(LicenseService.class));
        assertThat(expiryCheckNoEnterprise.validate(), is(true));
    }
}
