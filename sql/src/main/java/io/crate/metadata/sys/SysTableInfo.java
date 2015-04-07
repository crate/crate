/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.sys;

import io.crate.analyze.WhereClause;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.Routing;
import io.crate.metadata.table.AbstractTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.List;
import java.util.Map;

public abstract class SysTableInfo extends AbstractTableInfo {

    public static final String SCHEMA = "sys";

    protected final ClusterService clusterService;

    protected SysTableInfo(ClusterService clusterService, SysSchemaInfo sysSchemaInfo) {
        super(sysSchemaInfo);
        this.clusterService = clusterService;
    }

    protected Routing tableRouting(WhereClause whereClause) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        TreeMapBuilder<String, Map<String, List<Integer>>> builder = TreeMapBuilder.newMapBuilder();
        for (DiscoveryNode node : nodes) {
            builder.put(
                    node.id(),
                    TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(ident().fqn(), null).map()
            );
        }
        return new Routing(builder.map());
    }

    @Override
    public ColumnPolicy columnPolicy() {
        return ColumnPolicy.STRICT;
    }
}
