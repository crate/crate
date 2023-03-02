/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.reference.sys.check.node;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;

@Singleton
public class RecoveryAfterNodesSysCheck extends AbstractSysNodeCheck {

    private final ClusterService clusterService;
    private final Settings settings;

    static final int ID = 2;
    private static final String DESCRIPTION = "The cluster setting 'gateway.recover_after_data_nodes' " +
                                              "(or the deprecated `gateway.recovery_after_nodes` setting) " +
                                              "is not configured or it is set to a value that seems low in " +
                                              "relation to the the maximum/expected number of (data) nodes " +
                                              "in the cluster.";

    @Inject
    public RecoveryAfterNodesSysCheck(ClusterService clusterService, Settings settings) {
        super(ID, DESCRIPTION, Severity.MEDIUM);
        this.clusterService = clusterService;
        this.settings = settings;
    }

    @Override
    public boolean isValid() {
        int actualNodes = clusterService.state().nodes().getDataNodes().size();
        int afterNodes = GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.get(settings);
        int expectedNodes = GatewayService.EXPECTED_DATA_NODES_SETTING.get(settings);
        if (afterNodes == -1 || expectedNodes == -1) {
            // fallback to deprecated settings for BWC
            actualNodes = clusterService.state().nodes().getSize();
            afterNodes = GatewayService.RECOVER_AFTER_NODES_SETTING.get(settings);
            expectedNodes = GatewayService.EXPECTED_NODES_SETTING.get(settings);
        }

        return validate(afterNodes, expectedNodes, actualNodes);
    }

    private static boolean validate(int afterNodes, int expectedNodes, int actualNodes) {
        return actualNodes == 1
               || (expectedNodes / 2) < afterNodes && afterNodes <= expectedNodes;
    }

}
