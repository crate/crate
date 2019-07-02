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

import io.crate.Constants;
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
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.STRING_ARRAY;

public class PgDatabaseTable extends StaticTableInfo<Void> {

    public static final RelationName NAME = new RelationName(PgCatalogSchemaInfo.NAME, "pg_database");

    static Map<ColumnIdent, RowCollectExpressionFactory<Void>> expressions() {
        return columnRegistrar().expressions();
    }

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<Void> columnRegistrar() {
        return new ColumnRegistrar<Void>(NAME, RowGranularity.DOC)
            .register("oid", INTEGER, () -> constant(0))
            .register("datname", STRING, () -> constant(Constants.DB_NAME))
            .register("datdba", INTEGER, () -> constant(1))
            .register("encoding", INTEGER, () -> constant(6))
            .register("datcollate", STRING, () -> constant("en_US.UTF-8"))
            .register("datctype", STRING, () -> constant("en_US.UTF-8"))
            .register("datistemplate", BOOLEAN,() -> constant(false))
            .register("datallowconn", BOOLEAN, () -> constant(true))
            .register("datconnlimit", INTEGER,() -> constant(-1)) // no limit
            // We don't have any good values for these
            .register("datlastsysoid", INTEGER, () -> constant(null))
            .register("datfrozenxid", INTEGER, () -> constant(null))
            .register("datminmxid", INTEGER, () -> constant(null))
            .register("dattablespace", INTEGER, () -> constant(null))
            .register("datacl", STRING_ARRAY, () -> constant(null));
    }

    PgDatabaseTable() {
        super(NAME, columnRegistrar());
    }

    @Override
    public Routing getRouting(ClusterState state, RoutingProvider routingProvider, WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection, SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(NAME, state.getNodes().getLocalNodeId());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }
}
