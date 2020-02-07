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
import io.crate.metadata.table.StaticTableInfo;
import io.crate.protocols.postgres.types.PGType;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.metadata.pgcatalog.OidHash.schemaOid;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;

public class PgTypeTable extends StaticTableInfo<PGType> {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_type");

    private static final Integer TYPE_NAMESPACE_OID = schemaOid(PgCatalogSchemaInfo.NAME);

    private static final String TYPTYPE = "b";

    static Map<ColumnIdent, RowCollectExpressionFactory<PGType>> expressions() {
        return columnRegistrar().expressions();
    }

    private static ColumnRegistrar<PGType> columnRegistrar() {
        return new ColumnRegistrar<PGType>(IDENT, RowGranularity.DOC)
            .register("oid", INTEGER, () -> forFunction(PGType::oid))
            .register("typname", STRING, () -> forFunction(PGType::typName))
            .register("typdelim", STRING, () -> forFunction(PGType::typDelim))
            .register("typelem", INTEGER, () -> forFunction(PGType::typElem))
            .register("typlen", SHORT, () -> forFunction(PGType::typeLen))
            .register("typbyval", BOOLEAN, () -> constant(true))
            .register("typtype", STRING, () -> constant(TYPTYPE))
            .register("typcategory", STRING, () -> forFunction(PGType::typeCategory))
            .register("typowner", INTEGER, () -> constant(null))
            .register("typisdefined", BOOLEAN, () -> constant(true))
            // Zero for non-composite types, otherwise should point
            // to the pg_class table entry.
            .register("typrelid", INTEGER, () -> constant(0))
            .register("typndims", INTEGER, () -> constant(0))
            .register("typcollation", INTEGER, () -> constant(0))
            .register("typdefault", STRING, () -> constant(null))
            .register("typbasetype", INTEGER, () -> constant(0))
            .register("typtypmod", INTEGER, () -> constant(-1))
            .register("typnamespace", INTEGER, () -> constant(TYPE_NAMESPACE_OID))
            .register("typarray", INTEGER, () -> forFunction(PGType::typArray))
            .register("typinput", STRING, () -> forFunction(t -> {
                if (t.typArray() == 0) {
                    return "array_in";
                } else {
                    return t.typName() + "_in";
                }
            }))
            .register("typoutput", STRING, () -> forFunction(t -> {
                if (t.typArray() == 0) {
                    return "array_out";
                } else {
                    return t.typName() + "_out";
                }
            }))
            .register("typnotnull", BOOLEAN, () -> constant(false));
    }

    PgTypeTable() {
        super(IDENT, columnRegistrar());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
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
