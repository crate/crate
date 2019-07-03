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
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.LONG;

public class SysHealthTableInfo extends StaticTableInfo<TableHealth> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "health");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    static Map<ColumnIdent, RowCollectExpressionFactory<TableHealth>> expressions() {
        return columnRegistrar().expressions();
    }

    private static ColumnRegistrar<TableHealth> columnRegistrar() {
        return new ColumnRegistrar<TableHealth>(IDENT, GRANULARITY)
            .register("table_name", STRING, () -> forFunction(TableHealth::getTableName))
            .register("table_schema", STRING, () -> forFunction(TableHealth::getTableSchema))
            .register("partition_ident",STRING, () -> forFunction(TableHealth::getPartitionIdent))
            .register("health", STRING, () -> forFunction(TableHealth::getHealth))
            .register("severity", SHORT, () -> forFunction(TableHealth::getSeverity))
            .register("missing_shards", LONG, () -> forFunction(TableHealth::getMissingShards))
            .register("underreplicated_shards", LONG, () -> forFunction(TableHealth::getUnderreplicatedShards));
    }

    SysHealthTableInfo() {
        super(IDENT, columnRegistrar());
    }

    @Override
    public Routing getRouting(ClusterState clusterState,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, clusterState.getNodes().getLocalNodeId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }
}
