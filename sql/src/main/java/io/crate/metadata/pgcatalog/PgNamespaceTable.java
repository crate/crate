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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.ArrayType;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.metadata.pgcatalog.OidHash.schemaOid;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.INTEGER;

public class PgNamespaceTable extends StaticTableInfo<SchemaInfo> {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_namespace");

    static Map<ColumnIdent, RowCollectExpressionFactory<SchemaInfo>> expressions() {
        return columnRegistrar().expressions();
    }

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<SchemaInfo> columnRegistrar() {
        return new ColumnRegistrar<SchemaInfo>(IDENT, RowGranularity.DOC)
            .register("oid", INTEGER, () -> forFunction(s -> schemaOid(s.name())))
            .register("nspname", STRING, () -> forFunction(SchemaInfo::name))
            .register("nspowner", INTEGER, () -> constant(0))
            .register("nspacl", new ArrayType(ObjectType.untyped()), () -> constant(null));
    }

    PgNamespaceTable() {
        super(IDENT, columnRegistrar());
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
