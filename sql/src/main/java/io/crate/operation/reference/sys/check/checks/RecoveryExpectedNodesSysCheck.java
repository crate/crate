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
import org.elasticsearch.common.settings.Settings;

@Singleton
public class RecoveryExpectedNodesSysCheck extends AbstractSysCheck {

    private final ClusterService clusterService;
    private final Settings settings;

    private static final int ID = 2;
    private static final String DESCRIPTION = "The value of the cluster setting 'gateway.expected_nodes' " +
            "must be equal to the maximum/expected number of nodes in the cluster.";

    @Inject
    public RecoveryExpectedNodesSysCheck(ClusterService clusterService, Settings settings) {
        super(ID, new BytesRef(DESCRIPTION), Severity.HIGH);
        this.clusterService = clusterService;
        this.settings = settings;
    }

    @Override
    public boolean validate() {
        return validate(clusterService.state().nodes().getSize(),
                CrateSettings.GATEWAY_EXPECTED_NODES.extract(settings)
        );
    }

    protected boolean validate(int clusterSize, int expectedNodes) {
        return clusterSize == 1 || clusterSize == expectedNodes;
    }
}
