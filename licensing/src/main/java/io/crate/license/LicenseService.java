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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;

@Singleton
public class LicenseService {

    private static final Logger LOGGER = Loggers.getLogger(LicenseService.class);

    private final ClusterService clusterService;

    @Inject
    public LicenseService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    boolean validateLicense(final LicenseMetaData license) {
        // todo: implement
        // 1. check for future expiry date
        // 2. validate signature
        return true;
    }


    void registerLicense(final LicenseMetaData licenseMetaData,
                         final ActionListener<SetLicenseResponse> listener,
                         final TimeValue timeout) {
        clusterService.submitStateUpdateTask("register license to [" + licenseMetaData.issuedTo() + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                    mdBuilder.putCustom(LicenseMetaData.TYPE, licenseMetaData);
                    return ClusterState.builder(currentState).metaData(mdBuilder).build();
                }

                @Override
                public TimeValue timeout() {
                    return timeout;
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new SetLicenseResponse(true));
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }
            });
    }
}
