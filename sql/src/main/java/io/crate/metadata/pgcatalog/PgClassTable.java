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
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.ArrayType;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.metadata.pgcatalog.OidHash.schemaOid;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.FLOAT;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING_ARRAY;

public class PgClassTable extends StaticTableInfo<RelationInfo> {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_class");
    private static final String KIND_TABLE = "r";
    private static final String KIND_VIEW = "v";

    private static final String PERSISTENCE_PERMANENT = "p";

    static Map<ColumnIdent, RowCollectExpressionFactory<RelationInfo>> expressions() {
        return columnRegistrar().expressions();
    }

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<RelationInfo> columnRegistrar() {
        return new ColumnRegistrar<RelationInfo>(IDENT, RowGranularity.DOC)
            .register("oid", INTEGER, () -> forFunction(OidHash::relationOid))
            .register("relname", STRING, () -> forFunction(r -> r.ident().name()))
            .register("relnamespace", INTEGER,() -> forFunction(r -> schemaOid(r.ident().schema())))
            .register("reltype", INTEGER, () -> constant(0))
            .register("reloftype", INTEGER, () -> constant(0))
            .register("relowner", INTEGER, () -> constant(0))
            .register("relam", INTEGER, () -> constant(0))
            .register("relfilenode", INTEGER, () -> constant(0))
            .register("reltablespace", INTEGER, () -> constant(0))
            .register("relpages", INTEGER, () -> constant(0))
            .register("reltuples", FLOAT, () -> constant(0.0f))
            .register("relallvisible", INTEGER, () -> constant(0))
            .register("reltoastrelid", INTEGER, () -> constant(0))
            .register("relhasindex", BOOLEAN, () -> constant(false))
            .register("relisshared", BOOLEAN, () -> constant(false))
            .register("relpersistence", STRING, () -> constant(PERSISTENCE_PERMANENT))
            .register("relkind", STRING, () -> forFunction(r -> r.relationType() == RelationType.VIEW ? KIND_VIEW : KIND_TABLE))
            .register("relnatts", SHORT, () -> forFunction(r -> (short) r.columns().size()))
            .register("relchecks", SHORT, () -> constant((short) 0))
            .register("relhasoids", BOOLEAN, () -> constant(false))
            .register("relhaspkey", BOOLEAN, () -> forFunction(r -> r.primaryKey().size() > 0))
            .register("relhasrules", BOOLEAN, () -> constant(false))
            .register("relhastriggers", BOOLEAN, () -> constant(false))
            .register("relhassubclass", BOOLEAN, () -> constant(false))
            .register("relrowsecurity", BOOLEAN, () -> constant(false))
            .register("relforcerowsecurity", BOOLEAN, () -> constant(false))
            .register("relispopulated", BOOLEAN, () -> constant(true))
            .register("relreplident", STRING, () -> constant("p"))
            .register("relispartition", BOOLEAN, () -> constant(false))
            .register("relfrozenxid", INTEGER,() -> constant(0))
            .register("relminmxid", INTEGER, () -> constant(0))
            .register("relacl", new ArrayType(ObjectType.untyped()), () -> constant(null))
            .register("reloptions", STRING_ARRAY, () -> constant(null))
            .register("relpartbound", ObjectType.untyped(), () -> constant(null));
    }

    PgClassTable() {
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
