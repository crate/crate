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

package io.crate.operation.reference.sys.check.checks;

import io.crate.metadata.settings.CrateSettings;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.node.settings.NodeSettingsService;

@Singleton
public class MinMasterNodesSysCheck extends AbstractSysCheck {

    private final ClusterService clusterService;

    private static final int ID = 1;
    private static final String DESCRIPTION = "The minimum number of master nodes must be greater" +
            " than the half of maximum number of nodes in the cluster ";

    @Inject
    public MinMasterNodesSysCheck(ClusterService clusterService) {
        super(ID, new BytesRef(DESCRIPTION), Severity.INFO, true);
        this.clusterService = clusterService;
    }

    @Override
    public boolean validate() {
        return validate(
                clusterService.state().nodes().getSize(),
                CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.extract(NodeSettingsService.getGlobalSettings())
        );
    }

    protected boolean validate( int clusterSize, int minMasterNodes) {
        passed = ((clusterSize / 2) + 1) <= minMasterNodes && minMasterNodes <= clusterSize;
        severity = passed ? Severity.INFO : Severity.ERROR;
        return passed;
    }
}
