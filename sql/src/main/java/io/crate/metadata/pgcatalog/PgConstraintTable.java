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
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.metadata.pgcatalog.OidHash.constraintOid;
import static io.crate.metadata.pgcatalog.OidHash.relationOid;
import static io.crate.metadata.pgcatalog.OidHash.schemaOid;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.SHORT_ARRAY;
import static io.crate.types.DataTypes.INTEGER_ARRAY;
import static io.crate.types.DataTypes.INTEGER;

public class PgConstraintTable extends StaticTableInfo<ConstraintInfo> {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_constraint");

    private static final String NO_ACTION = "a";
    private static final String MATCH_SIMPLE = "s";

    static Map<ColumnIdent, RowCollectExpressionFactory<ConstraintInfo>> expressions() {
        return columnRegistrar().expressions();
    }

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<ConstraintInfo> columnRegistrar() {
        return new ColumnRegistrar<ConstraintInfo>(IDENT, RowGranularity.DOC)
           .register("oid", INTEGER, () -> forFunction(c -> constraintOid(c.relationName().fqn(), c.constraintName(), c.constraintType().toString())))
           .register("conname", STRING, () -> forFunction(ConstraintInfo::constraintName))
           .register("connamespace", INTEGER, () -> forFunction(c -> schemaOid(c.relationName().schema())))
           .register("contype", STRING, () -> forFunction(c -> c.constraintType().postgresChar()))
           .register("condeferrable", BOOLEAN, () -> constant(false))
           .register("condeferred", BOOLEAN, () -> constant(false))
           .register("convalidated", BOOLEAN, () -> constant(true))
           .register("conrelid", INTEGER, () -> forFunction(c -> relationOid(c.relationInfo())))
           .register("contypid", INTEGER, () -> constant(0))
           .register("conindid", INTEGER, () -> constant(0))
           .register("confrelid", INTEGER, () -> constant(0))
           .register("confupdtype", STRING, () -> constant(NO_ACTION))
           .register("confdeltype", STRING, () -> constant(NO_ACTION))
           .register("confmatchtype", STRING, () -> constant(MATCH_SIMPLE))
           .register("conislocal", BOOLEAN, () -> constant(true))
           .register("coninhcount", INTEGER, () -> constant(0))
           .register("connoinherit", BOOLEAN, () -> constant(true))
           .register("conkey", SHORT_ARRAY, () -> constant(null))
           .register("confkey", SHORT_ARRAY, () -> constant(null))
           .register("conpfeqop", INTEGER_ARRAY, () -> constant(null))
           .register("conppeqop", INTEGER_ARRAY, () -> constant(null))
           .register("conffeqop", INTEGER_ARRAY, () -> constant(null))
           .register("conexclop", INTEGER_ARRAY, () -> constant(null))
           .register("conbin", ObjectType.untyped(), () -> constant(null))
           .register("consrc", STRING, () -> constant(null));
    }

    PgConstraintTable() {
        super(IDENT,columnRegistrar());
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
