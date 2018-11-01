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

package io.crate.expression.reference.sys.cluster;

import io.crate.expression.reference.NestedObjectExpression;
import io.crate.license.DecryptedLicenseData;
import io.crate.license.LicenseService;
import org.elasticsearch.common.inject.Inject;

import java.util.Map;

public class ClusterLicenseExpression extends NestedObjectExpression {

    public static final String NAME = "license";
    private final LicenseService licenseService;

    @Inject
    public ClusterLicenseExpression(LicenseService licenseService) {
        this.licenseService = licenseService;
    }

    @Override
    public Map<String, Object> value() {
        DecryptedLicenseData decryptedLicenseData = licenseService.currentLicense();
        if (decryptedLicenseData != null) {
            childImplementations.put(DecryptedLicenseData.EXPIRATION_DATE_IN_MS, () -> decryptedLicenseData.expirationDateInMs());
            childImplementations.put(DecryptedLicenseData.ISSUED_TO, () -> decryptedLicenseData.issuedTo());
            return super.value();
        } else {
            return null;
        }
    }
}
