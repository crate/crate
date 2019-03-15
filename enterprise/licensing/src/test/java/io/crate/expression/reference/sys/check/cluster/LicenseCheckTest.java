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

package io.crate.expression.reference.sys.check.cluster;

import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.license.LicenseData;
import io.crate.license.LicenseService;
import io.crate.settings.SharedSettings;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class LicenseCheckTest extends CrateDummyClusterServiceUnitTest {

    private LicenseService licenseService;
    private LicenseCheck licenseCheck;

    @Before
    public void setupLicenseCheck() {
        licenseService = mock(LicenseService.class);
        Settings settings = Settings.builder().put("license.enterprise", true).build();
        licenseCheck = new LicenseCheck(settings, licenseService, clusterService);
    }

    @After
    public void assertSettingDeprecation() {
        assertSettingDeprecationsAndWarnings(new Setting[] {SharedSettings.ENTERPRISE_LICENSE_SETTING.setting() });
    }

    @Test
    public void testSysCheckMetadata() {
        assertThat(licenseCheck.id(), is(6));
    }

    @Test
    public void testValidLicense() {
        LicenseData thirtyDaysLicense = new LicenseData(
            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(30), "test", 2);
        when(licenseService.getLicenseState()).thenReturn(LicenseService.LicenseState.VALID);
        when(licenseService.currentLicense()).thenReturn(thirtyDaysLicense);
        assertThat(licenseCheck.validate(), is(true));
    }

    @Test
    public void testLessThanFifteenDaysToExpiryTriggersMediumCheck() {
        LicenseData sevenDaysLicense = new LicenseData(
            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(7), "test", 2);

        when(licenseService.getLicenseState()).thenReturn(LicenseService.LicenseState.EXPIRED);
        when(licenseService.currentLicense()).thenReturn(sevenDaysLicense);
        assertThat(licenseCheck.validate(), is(false));
        assertThat(licenseCheck.severity(), is(SysCheck.Severity.MEDIUM));
    }

    @Test
    public void testLessThanOneDayToExpiryTriggersSevereCheck() {
        LicenseData sevenDaysLicense = new LicenseData(
            System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15), "test", 2);

        when(licenseService.getLicenseState()).thenReturn(LicenseService.LicenseState.EXPIRED);
        when(licenseService.currentLicense()).thenReturn(sevenDaysLicense);
        assertThat(licenseCheck.validate(), is(false));
        assertThat(licenseCheck.severity(), is(SysCheck.Severity.HIGH));
    }

    @Test
    public void testCheckFailsOnMaxNodesViolation() {
        LicenseData license = new LicenseData(
            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(40), "test", 2);
        when(licenseService.currentLicense()).thenReturn(license);
        when(licenseService.getLicenseState()).thenReturn(LicenseService.LicenseState.MAX_NODES_VIOLATED);
        assertThat(licenseCheck.validate(), is(false));
        assertThat(licenseCheck.severity(), is(SysCheck.Severity.HIGH));
    }

    @Test
    public void testCheckIsAlwaysValidWhenEnterpriseIsDisabled() {
        Settings settings = Settings.builder().put("license.enterprise", false).build();
        LicenseCheck expiryCheckNoEnterprise = new LicenseCheck(settings, mock(LicenseService.class), clusterService);
        assertThat(expiryCheckNoEnterprise.validate(), is(true));
    }
}
