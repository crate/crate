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
import io.crate.license.LicenseExpiryNotification;
import io.crate.license.LicenseService;
import io.crate.settings.SharedSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import static io.crate.expression.reference.sys.check.AbstractSysCheck.CLUSTER_CHECK_LINK_PATTERN;
import static io.crate.expression.reference.sys.check.AbstractSysCheck.getLinkedDescription;
import static io.crate.expression.reference.sys.check.SysCheck.Severity.HIGH;
import static io.crate.expression.reference.sys.check.SysCheck.Severity.LOW;
import static io.crate.expression.reference.sys.check.SysCheck.Severity.MEDIUM;
import static io.crate.license.LicenseExpiryNotification.MODERATE;

@Singleton
public class LicenseCheck implements SysCheck {

    private static final int ID = 6;
    private static final String LICENSE_OK = "Your CrateDB license is valid. Enjoy CrateDB!";
    private static final String LICENSE_NA_COMMUNITY_DESCRIPTION = "CrateDB enterprise is not enabled. Enjoy the community edition!";
    private final boolean enterpriseEnabled;
    private final ClusterService clusterService;

    private String description;
    private Severity severity = Severity.LOW;
    private final LicenseService licenseService;

    @Inject
    public LicenseCheck(Settings settings, LicenseService licenseService, ClusterService clusterService) {
        enterpriseEnabled = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
        this.clusterService = clusterService;
        this.description = LICENSE_NA_COMMUNITY_DESCRIPTION; // will be overwritten on validation if enterprise is enabled
        this.licenseService = licenseService;
    }

    @Override
    public boolean validate() {
        if (!enterpriseEnabled) {
            return true;
        }
        LicenseData currentLicense = licenseService.currentLicense();
        if (currentLicense == null) {
            // node might've not have received the license cluster state
            return true;
        }
        LicenseService.LicenseState licenseState = licenseService.getLicenseState();
        switch (licenseState) {
            case VALID:
                severity = LOW;
                description = LICENSE_OK;
                return true;

            case EXPIRED:
                LicenseExpiryNotification licenseExpiryNotification = LicenseExpiryNotification.of(currentLicense);
                description = getLinkedDescription(ID,
                    licenseExpiryNotification.notificationMessage(currentLicense.millisToExpiration()),
                    "For more information on Cluster Checks please visit: " + CLUSTER_CHECK_LINK_PATTERN);
                severity = licenseExpiryNotification.equals(MODERATE) ? MEDIUM : HIGH;
                return false;

            case MAX_NODES_VIOLATED:
                severity = HIGH;
                description = getLinkedDescription(ID, String.format(
                    Locale.ENGLISH,
                    "The license is limited to %d nodes, but there are %d nodes in the cluster. " +
                    "To upgrade your license visit https://crate.io/license-update/",
                    licenseService.currentLicense().maxNumberOfNodes(),
                    clusterService.state().getNodes().getSize()
                ), "For more information visit: " + CLUSTER_CHECK_LINK_PATTERN);
                return false;

            default:
                throw new AssertionError("Illegal license state: " + licenseState);
        }
    }

    @Override
    public CompletableFuture<?> computeResult() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String description() {
        return description;
    }

    @Override
    public Severity severity() {
        return severity;
    }
}
