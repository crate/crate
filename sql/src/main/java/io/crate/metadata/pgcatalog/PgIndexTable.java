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
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

public class PgIndexTable extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_index");

    static class Columns {
        static final ColumnIdent INDRELID = new ColumnIdent("indrelid");
        static final ColumnIdent INDEXRELID = new ColumnIdent("indexrelid");
        static final ColumnIdent INDNATTS = new ColumnIdent("indnatts");
        static final ColumnIdent INDISUNIQUE = new ColumnIdent("indisunique");
        static final ColumnIdent INDISPRIMARY = new ColumnIdent("indisprimary");
        static final ColumnIdent INDISEXCLUSION = new ColumnIdent("indisexclusion");
        static final ColumnIdent INDIMMEDIATE = new ColumnIdent("indimmediate");
        static final ColumnIdent INDISCLUSTERED = new ColumnIdent("indisclustered");
        static final ColumnIdent INDISVALID = new ColumnIdent("indisvalid");
        static final ColumnIdent INDCHECKXMIN = new ColumnIdent("indcheckxmin");
        static final ColumnIdent INDISREADY = new ColumnIdent("indisready");
        static final ColumnIdent INDISLIVE = new ColumnIdent("indislive");
        static final ColumnIdent INDISREPLIDENT = new ColumnIdent("indisreplident");
        static final ColumnIdent INDKEY = new ColumnIdent("indkey");
        static final ColumnIdent INDCOLLATION = new ColumnIdent("indcollation");
        static final ColumnIdent INDCLASS = new ColumnIdent("indclass");
        static final ColumnIdent INDOPTION = new ColumnIdent("indoption");
        static final ColumnIdent INDEXPRS = new ColumnIdent("indexprs");
        static final ColumnIdent INDPRED = new ColumnIdent("indpred");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<Void>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<Void>>builder()
            .put(Columns.INDRELID, () -> NestableCollectExpression.constant(0))
            .put(Columns.INDEXRELID, () -> NestableCollectExpression.constant(0))
            .put(Columns.INDNATTS, () -> NestableCollectExpression.constant((short) 0))
            .put(Columns.INDISUNIQUE, () -> NestableCollectExpression.constant(false))
            .put(Columns.INDISPRIMARY, () -> NestableCollectExpression.constant(false))
            .put(Columns.INDISEXCLUSION, () -> NestableCollectExpression.constant(false))
            .put(Columns.INDIMMEDIATE, () -> NestableCollectExpression.constant(true))
            .put(Columns.INDISCLUSTERED, () -> NestableCollectExpression.constant(false))
            .put(Columns.INDISVALID, () -> NestableCollectExpression.constant(true))
            .put(Columns.INDCHECKXMIN, () -> NestableCollectExpression.constant(false))
            .put(Columns.INDISREADY, () -> NestableCollectExpression.constant(true))
            .put(Columns.INDISLIVE, () -> NestableCollectExpression.constant(true))
            .put(Columns.INDISREPLIDENT, () -> NestableCollectExpression.constant(false))
            .put(Columns.INDKEY, () -> NestableCollectExpression.constant((short) 0))
            .put(Columns.INDCOLLATION, () -> NestableCollectExpression.constant(null))
            .put(Columns.INDCLASS, () -> NestableCollectExpression.constant(null))
            .put(Columns.INDOPTION, () -> NestableCollectExpression.constant(null))
            .put(Columns.INDEXPRS, () -> NestableCollectExpression.constant(null))
            .put(Columns.INDPRED, () -> NestableCollectExpression.constant(null))
            .build();
    }

    PgIndexTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.INDRELID.name(), DataTypes.INTEGER)
                .register(Columns.INDEXRELID.name(), DataTypes.INTEGER)
                .register(Columns.INDNATTS.name(), DataTypes.SHORT)
                .register(Columns.INDISUNIQUE.name(), DataTypes.BOOLEAN)
                .register(Columns.INDISPRIMARY.name(), DataTypes.BOOLEAN)
                .register(Columns.INDISEXCLUSION.name(), DataTypes.BOOLEAN)
                .register(Columns.INDIMMEDIATE.name(), DataTypes.BOOLEAN)
                .register(Columns.INDISCLUSTERED.name(), DataTypes.BOOLEAN)
                .register(Columns.INDISVALID.name(), DataTypes.BOOLEAN)
                .register(Columns.INDCHECKXMIN.name(), DataTypes.BOOLEAN)
                .register(Columns.INDISREADY.name(), DataTypes.BOOLEAN)
                .register(Columns.INDISLIVE.name(), DataTypes.BOOLEAN)
                .register(Columns.INDISREPLIDENT.name(), DataTypes.BOOLEAN)
                .register(Columns.INDKEY.name(), DataTypes.SHORT_ARRAY)
                .register(Columns.INDCOLLATION.name(), DataTypes.INTEGER_ARRAY)
                .register(Columns.INDCLASS.name(), DataTypes.INTEGER_ARRAY)
                .register(Columns.INDOPTION.name(), DataTypes.SHORT_ARRAY)
                .register(Columns.INDEXPRS.name(), ObjectType.untyped())
                .register(Columns.INDPRED.name(), ObjectType.untyped()),
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
