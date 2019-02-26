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

package io.crate.metadata.pgcatalog;

import com.google.common.collect.ImmutableMap;
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
import io.crate.types.DataTypes;
import io.crate.es.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;

public final class PgDescriptionTable extends StaticTableInfo {

    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_description");

    static class Columns {
        private static final ColumnIdent OBJOID = new ColumnIdent("objoid");
        private static final ColumnIdent CLASSOID = new ColumnIdent("classoid");
        private static final ColumnIdent OBJSUBID = new ColumnIdent("objsubid");
        private static final ColumnIdent DESCRIPTION = new ColumnIdent("description");
    }

    PgDescriptionTable() {
        super(NAME, new ColumnRegistrar(NAME, RowGranularity.DOC)
            .register(Columns.OBJOID, DataTypes.INTEGER)
            .register(Columns.CLASSOID, DataTypes.INTEGER)
            .register(Columns.OBJSUBID, DataTypes.INTEGER)
            .register(Columns.DESCRIPTION, DataTypes.STRING),
            Collections.emptyList()
        );
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<Void>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<Void>>builder()
            .put(Columns.OBJOID, () -> constant(null))
            .put(Columns.CLASSOID, () -> constant(null))
            .put(Columns.OBJSUBID, () -> constant(null))
            .put(Columns.DESCRIPTION, () -> constant(null))
            .build();
    }

    @Override
    public Routing getRouting(ClusterState state, RoutingProvider routingProvider, WhereClause whereClause, RoutingProvider.ShardSelection shardSelection, SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(NAME, state.getNodes().getLocalNodeId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }
}
