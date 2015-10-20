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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.settings.CrateSettings;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.operation.reference.sys.cluster.ClusterSettingsExpression;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class MinMasterNodesSysCheck extends AbstractSysCheck {

    private final ClusterService clusterService;
    private final NestedReferenceResolver nestedReferenceResolver;

    private static final int ID = 1;
    private static final String DESCRIPTION = "The value of the cluster setting 'discovery.zen.minimum_master_nodes' " +
            "needs to be greater than half of the maximum/expected number of nodes and equal or less than " +
            "the maximum/expected number of nodes in the cluster.";

    private static final ReferenceInfo MIN_MASTER_NODES_REFERENCE_INFO = new ReferenceInfo(
            new ReferenceIdent(
                    SysClusterTableInfo.IDENT,
                    ClusterSettingsExpression.NAME,
                    Lists.newArrayList(Splitter.on(".")
                            .split(CrateSettings.DISCOVERY_ZEN_MIN_MASTER_NODES.settingName()))
            ),
            RowGranularity.DOC, DataTypes.INTEGER
    );

    @Inject
    public MinMasterNodesSysCheck(ClusterService clusterService, NestedReferenceResolver nestedReferenceResolver) {
        super(ID, new BytesRef(DESCRIPTION), Severity.HIGH);
        this.clusterService = clusterService;
        this.nestedReferenceResolver = nestedReferenceResolver;
    }

    @Override
    public boolean validate() {
        return validate(clusterService.state().nodes().getSize(),
                (Integer) nestedReferenceResolver.getImplementation(MIN_MASTER_NODES_REFERENCE_INFO).value());
    }

    protected boolean validate( int clusterSize, int minMasterNodes) {
        return clusterSize == 1
                || ((clusterSize / 2) + 1) <= minMasterNodes && minMasterNodes <= clusterSize;
    }

}
