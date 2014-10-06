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

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexReferenceInfo;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.table.AbstractTableInfo;
import io.crate.planner.symbol.DynamicReference;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.MapBuilder;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

public abstract class SysTableInfo extends AbstractTableInfo {

    public static final String SCHEMA = "sys";

    protected final ClusterService clusterService;

    protected SysTableInfo(ClusterService clusterService, SysSchemaInfo sysSchemaInfo) {
        super(sysSchemaInfo);
        this.clusterService = clusterService;
    }

    @Nullable
    @Override
    public IndexReferenceInfo getIndexReferenceInfo(ColumnIdent columnIdent) {
        return null;
    }

    protected Routing tableRouting(WhereClause whereClause) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        ImmutableMap.Builder<String, Map<String, Set<Integer>>> builder = ImmutableMap.builder();
        for (DiscoveryNode node : nodes) {
            builder.put(
                    node.id(),
                    MapBuilder.<String, Set<Integer>>newMapBuilder().put(ident().fqn(), null).map()
            );
        }
        return new Routing(builder.build());
    }

    @Override
    public DynamicReference dynamicReference(ColumnIdent columnIdent) {
        return new DynamicReference(new ReferenceIdent(ident(), columnIdent), rowGranularity());
    }
}
