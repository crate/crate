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

import io.crate.expression.NestableInput;
import io.crate.expression.reference.NestedObjectExpression;
import io.crate.license.LicenseData;
import io.crate.license.LicenseService;
import org.elasticsearch.common.inject.Inject;

import java.util.Map;

public class ClusterLicenseExpression extends NestedObjectExpression {

    public static final String NAME = "license";
    public static final String EXPIRY_DATE = "expiry_date";
    public static final String ISSUED_TO = "issued_to";
    public static final String MAX_NODES = "max_nodes";

    private final LicenseService licenseService;

    @Inject
    public ClusterLicenseExpression(LicenseService licenseService) {
        this.licenseService = licenseService;
    }

    @Override
    public NestableInput getChild(String name) {
        LicenseData licenseData = licenseService.currentLicense();
        fillChildImplementations(licenseData);
        return super.getChild(name);
    }

    private void fillChildImplementations(LicenseData licenseData) {
        if (licenseData != null) {
            childImplementations.put(EXPIRY_DATE, () -> {
                if (licenseData.expiryDateInMs() == Long.MAX_VALUE) {
                    return null;
                } else {
                    return licenseData.expiryDateInMs();
                }
            });
            childImplementations.put(ISSUED_TO, licenseData::issuedTo);
            childImplementations.put(MAX_NODES, licenseData::maxNumberOfNodes);
        } else {
            childImplementations.put(EXPIRY_DATE, () -> null);
            childImplementations.put(ISSUED_TO, () -> null);
            childImplementations.put(MAX_NODES, () -> null);
        }
    }

    @Override
    public Map<String, Object> value() {
        LicenseData licenseData = licenseService.currentLicense();
        if (licenseData == null) {
            return null;
        }

        fillChildImplementations(licenseData);
        return super.value();
    }
}
