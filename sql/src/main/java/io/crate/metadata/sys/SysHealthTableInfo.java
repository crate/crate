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

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;

public class SysHealthTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "health");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public static class Columns {
        static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        static final ColumnIdent HEALTH = new ColumnIdent("health");
        static final ColumnIdent SEVERITY = new ColumnIdent("severity");
        static final ColumnIdent MISSING_SHARDS = new ColumnIdent("missing_shards");
        static final ColumnIdent UNDERREPLICATED_SHARDS = new ColumnIdent("underreplicated_shards");
    }

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<TableHealth>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<TableHealth>>builder()
            .put(Columns.TABLE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(TableHealth::getTableName))
            .put(Columns.TABLE_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(TableHealth::getTableSchema))
            .put(Columns.PARTITION_IDENT,
                () -> RowContextCollectorExpression.objToBytesRef(TableHealth::getPartitionIdent))
            .put(Columns.HEALTH,
                () -> RowContextCollectorExpression.objToBytesRef(TableHealth::getHealth))
            .put(Columns.SEVERITY,
                () -> RowContextCollectorExpression.forFunction(TableHealth::getSeverity))
            .put(Columns.MISSING_SHARDS,
                () -> RowContextCollectorExpression.forFunction(TableHealth::getMissingShards))
            .put(Columns.UNDERREPLICATED_SHARDS,
                () -> RowContextCollectorExpression.forFunction(TableHealth::getUnderreplicatedShards))
            .build();
    }

    SysHealthTableInfo() {
        super(IDENT,
            new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.TABLE_NAME, DataTypes.STRING)
                .register(Columns.TABLE_SCHEMA, DataTypes.STRING)
                .register(Columns.PARTITION_IDENT, DataTypes.STRING)
                .register(Columns.HEALTH, DataTypes.STRING)
                .register(Columns.SEVERITY, DataTypes.SHORT)
                .register(Columns.MISSING_SHARDS, DataTypes.LONG)
                .register(Columns.UNDERREPLICATED_SHARDS, DataTypes.LONG),
            Collections.emptyList());
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
