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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import java.util.concurrent.CompletableFuture;

import static io.crate.expression.reference.sys.check.AbstractSysCheck.CLUSTER_CHECK_LINK_PATTERN;
import static io.crate.expression.reference.sys.check.AbstractSysCheck.getLinkedDescription;
import static io.crate.expression.reference.sys.check.SysCheck.Severity.HIGH;
import static io.crate.expression.reference.sys.check.SysCheck.Severity.MEDIUM;
import static io.crate.license.LicenseExpiryNotification.MODERATE;

@Singleton
public class LicenseExpiryCheck implements SysCheck {

    private static final int ID = 4;
    private static final BytesRef LICENSE_NOT_CLOSE_TO_EXPIRY_DESCRIPTION = new BytesRef("Your CrateDB license is not close to expiry. Enjoy CrateDB!");
    private final boolean enterpriseEnabled;

    private BytesRef description = LICENSE_NOT_CLOSE_TO_EXPIRY_DESCRIPTION;
    private Severity severity = Severity.LOW;
    private final LicenseService licenseService;

    @Inject
    public LicenseExpiryCheck(Settings settings, LicenseService licenseService) {
        enterpriseEnabled = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
        if (enterpriseEnabled == false) {
            description = new BytesRef("CrateDB enterprise is not enabled. Enjoy the community edition!");
        }
        this.licenseService = licenseService;
    }

    @Override
    public boolean validate() {
        if (!enterpriseEnabled) {
            return true;
        }

        DecryptedLicenseData currentLicense = licenseService.currentLicense();
        if (currentLicense == null) {
            // node might've not received the license cluster state
            return true;
        }

        LicenseExpiryNotification licenseExpiryNotification = licenseService.getLicenseExpiryNotification(currentLicense);

        if (licenseExpiryNotification != null) {
            description = getLinkedDescription(
                ID,
                licenseExpiryNotification.notificationMessage(currentLicense.millisToExpiration()),
                CLUSTER_CHECK_LINK_PATTERN
            );
            severity = licenseExpiryNotification.equals(MODERATE) ? MEDIUM : HIGH;
            return false;
        } else {
            description = LICENSE_NOT_CLOSE_TO_EXPIRY_DESCRIPTION;
            return true;
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
    public BytesRef description() {
        return description;
    }

    @Override
    public Severity severity() {
        return severity;
    }
}
