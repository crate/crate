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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.operation.reference.sys.check.AbstractSysCheck;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.discovery.zen.ElectMasterService;

@Singleton
public class MinMasterNodesSysCheck extends AbstractSysCheck {

    private final ClusterService clusterService;
    private final ReferenceImplementation numMasterNodes;

    private static final int ID = 1;
    private static final String DESCRIPTION = "The setting 'discovery.zen.minimum_master_nodes' " +
                                              "must not be less than half + 1 of eligible master nodes in the cluster. " +
                                              "It should be set to (number_master_nodes / 2) + 1.";

    private static final Reference MIN_MASTER_NODES_REFERENCE_INFO = new Reference(
        new ReferenceIdent(
            SysClusterTableInfo.IDENT,
            ClusterSettingsExpression.NAME,
            Lists.newArrayList(Splitter.on(".").split(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey()))
        ),
        RowGranularity.DOC, DataTypes.INTEGER
    );

    @Inject
    public MinMasterNodesSysCheck(ClusterService clusterService, ClusterReferenceResolver clusterReferenceResolver) {
        super(ID, DESCRIPTION, Severity.HIGH);
        this.clusterService = clusterService;
        this.numMasterNodes = clusterReferenceResolver.getImplementation(MIN_MASTER_NODES_REFERENCE_INFO);
    }

    @Override
    public boolean validate() {
        return validate(clusterService.state().nodes().getMasterNodes().size(), (Integer) numMasterNodes.value());
    }

    protected boolean validate(int masterNodes, int minMasterNodes) {
        return masterNodes == 1
               || ((masterNodes / 2) + 1) <= minMasterNodes && minMasterNodes <= masterNodes;
    }
}
