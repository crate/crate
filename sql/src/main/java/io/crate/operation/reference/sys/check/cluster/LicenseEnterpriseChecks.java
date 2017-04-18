/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.reference.sys.check.cluster;

import io.crate.operation.reference.sys.check.AbstractSysCheck;
import io.crate.settings.SharedSettings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;

@Singleton
public class LicenseEnterpriseChecks extends AbstractSysCheck {
    private static final int ID = 4;
    private static final String DESCRIPTION = "You are currently using the Enterprise Edition, " +
        "but have not configured a license. Please request a license or deactivate the Enterprise Edition.";

    private final boolean licenseEnterprise;
    private String licenseIdent;

    @Inject
    public LicenseEnterpriseChecks(ClusterSettings clusterSettings, Settings settings) {
        super(ID, DESCRIPTION, Severity.HIGH);
        licenseIdent = SharedSettings.LICENSE_IDENT_SETTING.setting().get(settings);
        clusterSettings.addSettingsUpdateConsumer(SharedSettings.LICENSE_IDENT_SETTING.setting(), this::setLicenseIdent);
        licenseEnterprise = SharedSettings.ENTERPRISE_LICENSE_SETTING.setting().get(settings);
    }

    private void setLicenseIdent(String licenseIdent) {
        this.licenseIdent = licenseIdent;
    }

    @Override
    public boolean validate() {
        if (licenseEnterprise){
            if (licenseIdent == null || "".equals(licenseIdent)){
                return false;
            }
            return true;
        }
        return true;
    }
}
