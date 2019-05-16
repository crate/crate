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
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LicenseCheckTest extends CrateDummyClusterServiceUnitTest {

    private LicenseService licenseService;
    private LicenseCheck licenseCheck;

    @Before
    public void setupLicenseCheck() {
        licenseService = mock(LicenseService.class);
        licenseCheck = new LicenseCheck(licenseService, clusterService);
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

        assertThat(licenseCheck.severity(), is(SysCheck.Severity.LOW));
        assertThat(licenseCheck.isValid(), is(true));
        assertThat(licenseCheck.description(), is(
            "Your CrateDB license is valid. Enjoy CrateDB!"));
    }

    @Test
    public void testLessThanFifteenDaysToExpiryTriggersMediumCheck() {
        LicenseData license = new LicenseData(
            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(7), "test", 2);

        when(licenseService.getLicenseState()).thenReturn(LicenseService.LicenseState.VALID);
        when(licenseService.currentLicense()).thenReturn(license);

        assertThat(licenseCheck.isValid(), is(false));
        assertThat(licenseCheck.severity(), is(SysCheck.Severity.MEDIUM));
        // Verify the description only partly as the LicenseData.millisToExpiration() can vary
        assertThat(licenseCheck.description(), containsString("Your CrateDB license will expire in"));
    }

    @Test
    public void testLessThanOneDayToExpiryTriggersSeverityHigh() {
        LicenseData license = new LicenseData(
            System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(15), "test", 2);
        when(licenseService.getLicenseState()).thenReturn(LicenseService.LicenseState.VALID);
        when(licenseService.currentLicense()).thenReturn(license);

        assertThat(licenseCheck.isValid(), is(false));
        assertThat(licenseCheck.severity(), is(SysCheck.Severity.HIGH));
        // Verify the description only partly as the LicenseData.millisToExpiration() can vary
        assertThat(licenseCheck.description(), containsString("Your CrateDB license will expire in"));
    }

    @Test
    public void testCheckFailsOnMaxNodesViolation() {
        LicenseData license = new LicenseData(
            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(40), "test", 2);
        when(licenseService.currentLicense()).thenReturn(license);
        when(licenseService.getLicenseState()).thenReturn(LicenseService.LicenseState.MAX_NODES_VIOLATED);

        assertThat(licenseCheck.isValid(), is(false));
        assertThat(licenseCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(licenseCheck.description(), is(
            "The license is limited to 2 nodes, but there are 1 nodes in the cluster." +
            " To upgrade your license visit https://crate.io/license-update/ For more information" +
            " visit: https://cr8.is/d-cluster-check-6"));
    }

    @Test
    public void testLicenseExpired() {
        LicenseData license = new LicenseData(0, "test", 2);

        when(licenseService.getLicenseState()).thenReturn(LicenseService.LicenseState.EXPIRED);
        when(licenseService.currentLicense()).thenReturn(license);

        assertThat(licenseCheck.isValid(), is(false));
        assertThat(licenseCheck.severity(), is(SysCheck.Severity.HIGH));
        assertThat(licenseCheck.description(), is(
            "Your CrateDB license has expired. For more information on " +
            "Licensing please visit: https://crate.io/license-update/?license=expired " +
            "For more information on Cluster Checks please visit: https://cr8.is/d-cluster-check-6"));
    }
}
