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
import io.crate.expression.reference.information.ColumnContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.protocols.postgres.types.PGTypes;
import io.crate.types.ArrayType;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.metadata.pgcatalog.OidHash.relationOid;
import static io.crate.types.DataTypes.isArray;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.STRING_ARRAY;

public class PgAttributeTable extends StaticTableInfo<ColumnContext> {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_attribute");

    static Map<ColumnIdent, RowCollectExpressionFactory<ColumnContext>> expressions() {
        return columnRegistrar().expressions();
    }

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<ColumnContext> columnRegistrar() {
        return new ColumnRegistrar<ColumnContext>(IDENT, RowGranularity.DOC)
            .register("attrelid", INTEGER, () -> forFunction(c -> relationOid(c.tableInfo)))
            .register("attname", STRING, () -> forFunction(c -> c.info.column().fqn()))
            .register("atttypid", INTEGER, () -> forFunction(c -> PGTypes.get(c.info.valueType()).oid()))
            .register("attstattarget", INTEGER, () -> constant(0))
            .register("attlen", SHORT, () -> forFunction(c -> PGTypes.get(c.info.valueType()).typeLen()))
            .register("attnum", INTEGER, () -> forFunction(c -> c.ordinal))
            .register("attndims", INTEGER, () -> forFunction(c -> isArray(c.info.valueType()) ? 1 : 0))
            .register("attcacheoff", INTEGER, () -> constant(-1))
            .register("atttypmod", INTEGER, () -> constant(-1))
            .register("attbyval", BOOLEAN, () -> constant(false))
            .register("attstorage", STRING, () -> constant(null))
            .register("attalign", STRING, () -> constant(null))
            .register("attnotnull", BOOLEAN, () -> forFunction(c -> !c.info.isNullable()))
            .register("atthasdef", BOOLEAN, () -> constant(false)) // don't support default values
            .register("attidentity", STRING, () -> constant(""))
            .register("attisdropped", BOOLEAN, () -> constant(false)) // don't support dropping columns
            .register("attislocal", BOOLEAN, () -> constant(true))
            .register("attinhcount", INTEGER, () -> constant(0))
            .register("attcollation", INTEGER, () -> constant(0))
            .register("attacl", new ArrayType(ObjectType.untyped()),() -> constant(null))
            .register("attoptions", STRING_ARRAY, () -> constant(null))
            .register("attfdwoptions", STRING_ARRAY, () -> constant(null));
    }

    PgAttributeTable() {
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
