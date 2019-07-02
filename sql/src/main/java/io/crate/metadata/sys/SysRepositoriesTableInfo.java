/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.sys;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.Repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.STRING;

public class SysRepositoriesTableInfo extends StaticTableInfo<Repository> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "repositories");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    static Map<ColumnIdent, RowCollectExpressionFactory<Repository>> expressions(List<Setting<?>> maskedSettings) {
        return columnRegistrar(maskedSettings).expressions();
    }

    static ColumnRegistrar<Repository> columnRegistrar(List<Setting<?>> maskedSettings) {
        return new ColumnRegistrar<Repository>(IDENT, GRANULARITY)
            .register("name", STRING, () -> forFunction((Repository r) -> r.getMetadata().name()))
            .register("type", STRING, () -> forFunction((Repository r) -> r.getMetadata().type()))
            .register("settings", ObjectType.untyped(), () ->
                forFunction((Repository r) -> r.getMetadata().settings().getAsStructuredMap(
                    maskedSettings.stream().map(Setting::getKey).collect(Collectors.toSet()))));
    }

    SysRepositoriesTableInfo(List<Setting<?>> maskedSettings) {
        super(IDENT, columnRegistrar(maskedSettings), "name");
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(ClusterState clusterState,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return routingProvider.forRandomMasterOrDataNode(IDENT, clusterState.getNodes());
    }
}
