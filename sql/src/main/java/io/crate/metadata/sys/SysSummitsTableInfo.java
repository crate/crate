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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.execution.engine.collect.files.SummitsContext;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.List;
import java.util.Map;

public class SysSummitsTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "summits");
    private static final List<ColumnIdent> PRIMARY_KEYS = ImmutableList.of(Columns.MOUNTAIN);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public static class Columns {
        static final ColumnIdent MOUNTAIN = new ColumnIdent("mountain");
        static final ColumnIdent HEIGHT = new ColumnIdent("height");
        static final ColumnIdent PROMINENCE = new ColumnIdent("prominence");
        static final ColumnIdent COORDINATES = new ColumnIdent("coordinates");
        static final ColumnIdent RANGE = new ColumnIdent("range");
        static final ColumnIdent CLASSIFICATION = new ColumnIdent("classification");
        static final ColumnIdent REGION = new ColumnIdent("region");
        static final ColumnIdent COUNTRY = new ColumnIdent("country");
        static final ColumnIdent FIRST_ASCENT = new ColumnIdent("first_ascent");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<SummitsContext>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SummitsContext>>builder()
            .put(SysSummitsTableInfo.Columns.MOUNTAIN,
                () -> NestableCollectExpression.forFunction(SummitsContext::mountain))
            .put(SysSummitsTableInfo.Columns.HEIGHT,
                () -> NestableCollectExpression.forFunction(SummitsContext::height))
            .put(SysSummitsTableInfo.Columns.PROMINENCE,
                () -> NestableCollectExpression.forFunction(SummitsContext::prominence))
            .put(SysSummitsTableInfo.Columns.COORDINATES,
                () -> NestableCollectExpression.forFunction(SummitsContext::coordinates))
            .put(SysSummitsTableInfo.Columns.RANGE,
                () -> NestableCollectExpression.forFunction(SummitsContext::range))
            .put(SysSummitsTableInfo.Columns.CLASSIFICATION,
                () -> NestableCollectExpression.forFunction(SummitsContext::classification))
            .put(SysSummitsTableInfo.Columns.REGION,
                () -> NestableCollectExpression.forFunction(SummitsContext::region))
            .put(SysSummitsTableInfo.Columns.COUNTRY,
                () -> NestableCollectExpression.forFunction(SummitsContext::country))
            .put(SysSummitsTableInfo.Columns.FIRST_ASCENT,
                () -> NestableCollectExpression.forFunction(SummitsContext::firstAscent))
            .build();
    }

    public SysSummitsTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
            .register(Columns.MOUNTAIN, DataTypes.STRING)
            .register(Columns.HEIGHT, DataTypes.INTEGER)
            .register(Columns.PROMINENCE, DataTypes.INTEGER)
            .register(Columns.COORDINATES, DataTypes.GEO_POINT)
            .register(Columns.RANGE, DataTypes.STRING)
            .register(Columns.CLASSIFICATION, DataTypes.STRING)
            .register(Columns.REGION, DataTypes.STRING)
            .register(Columns.COUNTRY, DataTypes.STRING)
            .register(Columns.FIRST_ASCENT, DataTypes.INTEGER), PRIMARY_KEYS);
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
        return Routing.forTableOnSingleNode(IDENT, clusterState.getNodes().getLocalNodeId());
    }
}
