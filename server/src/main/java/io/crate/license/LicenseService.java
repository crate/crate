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

import org.elasticsearch.cluster.ClusterStateListener;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

public interface LicenseService {

    static final String LICENSE_IS_LOADING = "License is loading";

    enum LicenseState {
        VALID,
        EXPIRED,
        MAX_NODES_VIOLATED
    }

    enum Mode {
        CE,
        ENTERPRISE
    }


    CompletableFuture<Long> registerLicense(String licenseKey);

    LicenseState getLicenseState();

    @Nullable
    LicenseData currentLicense();

    @Nullable
    ClusterStateListener clusterStateListener();

    default Mode getMode() {
        return Mode.CE;
    }

    @Nullable
    default Long getExpiryDateInMs() {
        var currentLicense = currentLicense();
        if (currentLicense == null) {
            return null;
        }
        var expiryDateInMs = currentLicense.expiryDateInMs();
        return expiryDateInMs == Long.MAX_VALUE ? null : expiryDateInMs;
    }

    @Nullable
    default String getIssuedTo() {
        var currentLicense = currentLicense();
        if (currentLicense != null) {
            return currentLicense.issuedTo();
        } else if (getMode() == Mode.ENTERPRISE) {
            // The admin-ui will switch it's view between CE and Enterprise based on a non-null value of `issued_to`
            // To prevent a race-condition (wrong admin-ui view) on node startup, we set a dummy value here.
            return LICENSE_IS_LOADING;
        } else {
            return null;
        }
    }

    @Nullable
    default Integer getMaxNodes() {
        var currentLicense = currentLicense();
        if (currentLicense == null) {
            return null;
        }
        return currentLicense.maxNumberOfNodes();
    }
}
