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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import static io.crate.expression.reference.sys.check.AbstractSysCheck.CLUSTER_CHECK_LINK_PATTERN;
import static io.crate.expression.reference.sys.check.AbstractSysCheck.getLinkedDescription;
import static io.crate.expression.reference.sys.check.SysCheck.Severity.HIGH;
import static io.crate.expression.reference.sys.check.SysCheck.Severity.LOW;
import static io.crate.expression.reference.sys.check.SysCheck.Severity.MEDIUM;

@Singleton
public class LicenseCheck implements SysCheck {

    private static final int ID = 6;
    private final ClusterService clusterService;
    private final LicenseService licenseService;

    // description will be overwritten on validation
    private String description = LicenseExpiryNotification.LICENSE_VALID;
    private Severity severity = Severity.LOW;

    @Inject
    public LicenseCheck(LicenseService licenseService, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.licenseService = licenseService;
    }

    @Override
    public boolean isValid() {
        LicenseData currentLicense = licenseService.currentLicense();
        if (currentLicense == null) {
            // node might've not have received the license cluster state
            return true;
        }
        LicenseService.LicenseState licenseState = licenseService.getLicenseState();
        switch (licenseState) {
            case VALID:
                if (currentLicense.millisToExpiration() <= Duration.ofDays(1).toMillis()) {
                    description = buildDescription(currentLicense);
                    severity = HIGH;
                    return false;
                } else if (currentLicense.millisToExpiration() <= Duration.ofDays(15).toMillis()) {
                    description = buildDescription(currentLicense);
                    severity = MEDIUM;
                    return false;
                } else {
                    description = LicenseExpiryNotification.LICENSE_VALID;
                    severity = LOW;
                    return true;
                }
            case EXPIRED:
                description = buildDescription(currentLicense);
                severity = HIGH;
                return false;

            case MAX_NODES_VIOLATED:
                description = buildMaxNodeViolatedDescription(
                    currentLicense, clusterService.state().getNodes().getSize());
                severity = HIGH;
                return false;

            default:
                throw new AssertionError("Illegal license state: " + licenseState);
        }
    }

    private String buildDescription(LicenseData license) {
        LicenseExpiryNotification licenseExpiryNotification = LicenseExpiryNotification.of(license);
        return getLinkedDescription(ID,
            licenseExpiryNotification.message(license.millisToExpiration()),
            "For more information on Cluster Checks please visit: " + CLUSTER_CHECK_LINK_PATTERN);
    }

    private String buildMaxNodeViolatedDescription(LicenseData license, int clusterSize) {
        return getLinkedDescription(ID, String.format(
            Locale.ENGLISH,
            "The license is limited to %d nodes, but there are %d nodes in the cluster. " +
            "To upgrade your license visit https://crate.io/license-update/",
            license.maxNumberOfNodes(),
            clusterSize
        ), "For more information visit: " + CLUSTER_CHECK_LINK_PATTERN);
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
