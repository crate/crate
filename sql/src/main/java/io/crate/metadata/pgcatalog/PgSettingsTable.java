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
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.settings.session.NamedSessionSetting;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static java.util.Map.entry;


public class PgSettingsTable extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_settings");

    static class Columns {
        static final ColumnIdent NAME = new ColumnIdent("name");
        static final ColumnIdent SETTING = new ColumnIdent("setting");
        static final ColumnIdent UNIT = new ColumnIdent("unit");
        static final ColumnIdent CATEGORY = new ColumnIdent("category");
        static final ColumnIdent SHORT_DESC = new ColumnIdent("short_desc");
        static final ColumnIdent EXTRA_DESC = new ColumnIdent("extra_desc");
        static final ColumnIdent CONTEXT = new ColumnIdent("context");
        static final ColumnIdent VARTYPE = new ColumnIdent("vartype");
        static final ColumnIdent SOURCE = new ColumnIdent("source");
        static final ColumnIdent ENUMVALS = new ColumnIdent("enumvals");
        static final ColumnIdent MIN_VAL = new ColumnIdent("min_val");
        static final ColumnIdent MAX_VAL = new ColumnIdent("max_val");
        static final ColumnIdent BOOT_VAL = new ColumnIdent("boot_val");
        static final ColumnIdent RESET_VAL = new ColumnIdent("reset_val");
        static final ColumnIdent SOURCEFILE = new ColumnIdent("sourcefile");
        static final ColumnIdent SOURCELINE = new ColumnIdent("sourceline");
        static final ColumnIdent PENDING_RESTART = new ColumnIdent("pending_restart");
    }

    PgSettingsTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.NAME.name(), DataTypes.STRING)
                .register(Columns.SETTING.name(), DataTypes.STRING)
                .register(Columns.UNIT.name(), DataTypes.STRING)
                .register(Columns.CATEGORY.name(), DataTypes.STRING)
                .register(Columns.SHORT_DESC.name(), DataTypes.STRING)
                .register(Columns.EXTRA_DESC.name(), DataTypes.STRING)
                .register(Columns.CONTEXT.name(), DataTypes.STRING)
                .register(Columns.VARTYPE.name(), DataTypes.STRING)
                .register(Columns.SOURCE.name(), DataTypes.STRING)
                .register(Columns.ENUMVALS.name(), DataTypes.STRING_ARRAY)
                .register(Columns.MIN_VAL.name(), DataTypes.STRING)
                .register(Columns.MAX_VAL.name(), DataTypes.STRING)
                .register(Columns.BOOT_VAL.name(), DataTypes.STRING)
                .register(Columns.RESET_VAL.name(), DataTypes.STRING)
                .register(Columns.SOURCEFILE.name(), DataTypes.STRING)
                .register(Columns.SOURCELINE.name(), DataTypes.INTEGER)
                .register(Columns.PENDING_RESTART.name(), DataTypes.BOOLEAN),
            Collections.emptyList());
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<NamedSessionSetting>> expressions() {
        return Map.ofEntries(
            entry(Columns.NAME,
                () -> NestableCollectExpression.forFunction(NamedSessionSetting::name)),
            entry(Columns.SETTING,
                () -> NestableCollectExpression.forFunction(NamedSessionSetting::value)),
            entry(Columns.UNIT, () -> constant(null)),
            entry(Columns.CATEGORY, () -> constant(null)),
            entry(Columns.SHORT_DESC,
                () -> NestableCollectExpression.forFunction(NamedSessionSetting::description)),
            entry(Columns.EXTRA_DESC, () -> constant(null)),
            entry(Columns.CONTEXT, () -> constant(null)),
            entry(Columns.VARTYPE,
                () -> NestableCollectExpression.forFunction(NamedSessionSetting::type)),
            entry(Columns.ENUMVALS, () -> constant(null)),
            entry(Columns.SOURCE, () -> constant(null)),
            entry(Columns.MIN_VAL, () -> constant(null)),
            entry(Columns.MAX_VAL, () -> constant(null)),
            entry(Columns.BOOT_VAL,
                () -> NestableCollectExpression.forFunction(NamedSessionSetting::defaultValue)),
            entry(Columns.RESET_VAL,
                () -> NestableCollectExpression.forFunction(NamedSessionSetting::defaultValue)),
            entry(Columns.SOURCEFILE, () -> constant(null)),
            entry(Columns.SOURCELINE, () -> constant(null)),
            entry(Columns.PENDING_RESTART, () -> constant(null))
        );
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
