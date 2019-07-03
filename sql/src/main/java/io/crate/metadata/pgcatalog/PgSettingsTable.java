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
import io.crate.metadata.settings.session.NamedSessionSetting;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.BOOLEAN;


public class PgSettingsTable extends StaticTableInfo<NamedSessionSetting> {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_settings");

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<NamedSessionSetting> columnRegistrar() {
        return new ColumnRegistrar<NamedSessionSetting>(IDENT, RowGranularity.DOC)
            .register("name", STRING, () -> forFunction(NamedSessionSetting::name))
            .register("setting", STRING, () -> forFunction(NamedSessionSetting::value))
            .register("unit", STRING, () -> constant(null))
            .register("category", STRING, () -> constant(null))
            .register("short_desc", STRING, () -> forFunction(NamedSessionSetting::description))
            .register("extra_desc", STRING, () -> constant(null))
            .register("context", STRING, () -> constant(null))
            .register("vartype", STRING, () -> forFunction(NamedSessionSetting::type))
            .register("source", STRING, () -> constant(null))
            .register("enumvals", STRING_ARRAY, () -> constant(null))
            .register("min_val", STRING, () -> constant(null))
            .register("max_val", STRING, () -> constant(null))
            .register("boot_val", STRING, () -> forFunction(NamedSessionSetting::defaultValue))
            .register("reset_val", STRING, () -> forFunction(NamedSessionSetting::defaultValue))
            .register("sourcefile", STRING, () -> constant(null))
            .register("sourceline", INTEGER, () -> constant(null))
            .register("pending_restart", BOOLEAN, () -> constant(null));
    }

    PgSettingsTable() {
        super(IDENT, columnRegistrar());
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<NamedSessionSetting>> expressions() {
        return columnRegistrar().expressions();
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
