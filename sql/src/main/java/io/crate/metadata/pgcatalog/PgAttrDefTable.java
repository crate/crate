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
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

public class PgAttrDefTable extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_attrdef");

    static class Columns {
        static final ColumnIdent OID = new ColumnIdent("oid");
        static final ColumnIdent ADRELID = new ColumnIdent("adrelid");
        static final ColumnIdent ADNUM = new ColumnIdent("adnum");
        static final ColumnIdent ADBIN = new ColumnIdent("adbin");
        static final ColumnIdent ADSRC = new ColumnIdent("adsrc");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<Void>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<Void>>builder()
            .put(Columns.OID, () -> NestableCollectExpression.constant(0))
            .put(Columns.ADRELID, () -> NestableCollectExpression.constant(0))
            .put(Columns.ADNUM, () -> NestableCollectExpression.constant(0))
            .put(Columns.ADBIN, () -> NestableCollectExpression.constant(null))
            .put(Columns.ADSRC, () -> NestableCollectExpression.constant(null))
            .build();

    }

    PgAttrDefTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.OID.name(), DataTypes.INTEGER)
                .register(Columns.ADRELID.name(), DataTypes.INTEGER)
                .register(Columns.ADNUM.name(), DataTypes.INTEGER)
                .register(Columns.ADBIN.name(), DataTypes.STRING)
                .register(Columns.ADSRC.name(), DataTypes.STRING),
            Collections.emptyList());
    }

    @Override
    public Routing getRouting(ClusterState state,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, state.getNodes().getLocalNodeId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }
}
