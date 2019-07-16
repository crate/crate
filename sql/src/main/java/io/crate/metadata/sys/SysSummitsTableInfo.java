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
import io.crate.execution.engine.collect.files.SummitsContext;
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
import static io.crate.types.DataTypes.GEO_POINT;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;

public class SysSummitsTableInfo extends StaticTableInfo<SummitsContext> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "summits");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public static Map<ColumnIdent, RowCollectExpressionFactory<SummitsContext>> expressions() {
        return columnRegistrar().expressions();
    }

    public SysSummitsTableInfo() {
        super(IDENT, columnRegistrar(), "mountain");
    }

    private static ColumnRegistrar<SummitsContext> columnRegistrar() {
        return new ColumnRegistrar<SummitsContext>(IDENT, GRANULARITY)
            .register("mountain", STRING, () -> forFunction(SummitsContext::mountain))
            .register("height", INTEGER, () -> forFunction(SummitsContext::height))
            .register("prominence", INTEGER, () -> forFunction(SummitsContext::prominence))
            .register("coordinates", GEO_POINT, () -> forFunction(SummitsContext::coordinates))
            .register("range", STRING, () -> forFunction(SummitsContext::range))
            .register("classification", STRING, () -> forFunction(SummitsContext::classification))
            .register("region", STRING, () -> forFunction(SummitsContext::region))
            .register("country", STRING, () -> forFunction(SummitsContext::country))
            .register("first_ascent", INTEGER, () -> forFunction(SummitsContext::firstAscent));
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
