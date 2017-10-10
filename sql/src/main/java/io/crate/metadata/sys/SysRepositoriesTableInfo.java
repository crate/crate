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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.repositories.Repository;

public class SysRepositoriesTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "repositories");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(Columns.NAME);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public static class Columns {
        public static final ColumnIdent NAME = new ColumnIdent("name");
        public static final ColumnIdent TYPE = new ColumnIdent("type");
        static final ColumnIdent SETTINGS = new ColumnIdent("settings");
    }

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<Repository>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<Repository>>builder()
            .put(SysRepositoriesTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef((Repository r) -> r.getMetadata().name()))
            .put(SysRepositoriesTableInfo.Columns.TYPE,
                () -> RowContextCollectorExpression.objToBytesRef((Repository r) -> r.getMetadata().type()))
            .put(SysRepositoriesTableInfo.Columns.SETTINGS,
                () -> RowContextCollectorExpression
                    .forFunction((Repository r) -> r.getMetadata().settings().getAsStructuredMap()))
            .build();
    }


    SysRepositoriesTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
            .register(Columns.NAME, DataTypes.STRING)
            .register(Columns.TYPE, DataTypes.STRING)
            .register(Columns.SETTINGS, DataTypes.OBJECT), PRIMARY_KEY);
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
