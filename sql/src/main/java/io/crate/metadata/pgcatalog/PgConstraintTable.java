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
import io.crate.metadata.table.ConstraintInfo;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

import static io.crate.metadata.pgcatalog.OidHash.constraintOid;
import static io.crate.metadata.pgcatalog.OidHash.relationOid;
import static io.crate.metadata.pgcatalog.OidHash.schemaOid;

public class PgConstraintTable extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_constraint");

    private static final String NO_ACTION = "a";
    private static final String MATCH_SIMPLE = "s";

    static class Columns {
        static final ColumnIdent OID = new ColumnIdent("oid");
        static final ColumnIdent CONNAME = new ColumnIdent("conname");
        static final ColumnIdent CONNAMESPACE = new ColumnIdent("connamespace");
        static final ColumnIdent CONTYPE = new ColumnIdent("contype");
        static final ColumnIdent CONDEFERRABLE = new ColumnIdent("condeferrable");
        static final ColumnIdent CONDEFERRED = new ColumnIdent("condeferred");
        static final ColumnIdent CONVALIDATED = new ColumnIdent("convalidated");
        static final ColumnIdent CONRELID = new ColumnIdent("conrelid");
        static final ColumnIdent CONTYPID = new ColumnIdent("contypid");
        static final ColumnIdent CONINDID = new ColumnIdent("conindid");
        static final ColumnIdent CONFRELID = new ColumnIdent("confrelid");
        static final ColumnIdent CONFUPDTYPE = new ColumnIdent("confupdtype");
        static final ColumnIdent CONFDELTYPE = new ColumnIdent("confdeltype");
        static final ColumnIdent CONFMATCHTYPE = new ColumnIdent("confmatchtype");
        static final ColumnIdent CONISLOCAL = new ColumnIdent("conislocal");
        static final ColumnIdent CONINHCOUNT = new ColumnIdent("coninhcount");
        static final ColumnIdent CONNOINHERIT = new ColumnIdent("connoinherit");
        static final ColumnIdent CONKEY = new ColumnIdent("conkey");
        static final ColumnIdent CONFKEY = new ColumnIdent("confkey");
        static final ColumnIdent CONPFEQOP = new ColumnIdent("conpfeqop");
        static final ColumnIdent CONPPEQOP = new ColumnIdent("conppeqop");
        static final ColumnIdent CONFFEQOP = new ColumnIdent("conffeqop");
        static final ColumnIdent CONEXCLOP = new ColumnIdent("conexclop");
        static final ColumnIdent CONBIN = new ColumnIdent("conbin");
        static final ColumnIdent CONSRC = new ColumnIdent("consrc");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ConstraintInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ConstraintInfo>>builder()
            .put(Columns.OID, () -> NestableCollectExpression.forFunction(
                c -> constraintOid(c.relationName().fqn(), c.constraintName(), c.constraintType().toString())))
            .put(Columns.CONNAME, () -> NestableCollectExpression.forFunction(ConstraintInfo::constraintName))
            .put(Columns.CONNAMESPACE, () -> NestableCollectExpression.forFunction(c -> schemaOid(c.relationName().schema())))
            .put(Columns.CONTYPE, () -> NestableCollectExpression.forFunction(c -> c.constraintType().postgresChar()))
            .put(Columns.CONDEFERRABLE, () -> NestableCollectExpression.constant(false))
            .put(Columns.CONDEFERRED, () -> NestableCollectExpression.constant(false))
            .put(Columns.CONVALIDATED, () -> NestableCollectExpression.constant(true))
            .put(Columns.CONRELID, () -> NestableCollectExpression.forFunction(c -> relationOid(c.relationInfo())))
            .put(Columns.CONTYPID, () -> NestableCollectExpression.constant(0))
            .put(Columns.CONINDID, () -> NestableCollectExpression.constant(0))
            .put(Columns.CONFRELID, () -> NestableCollectExpression.constant(0))
            .put(Columns.CONFUPDTYPE, () -> NestableCollectExpression.constant(NO_ACTION))
            .put(Columns.CONFDELTYPE, () -> NestableCollectExpression.constant(NO_ACTION))
            .put(Columns.CONFMATCHTYPE, () -> NestableCollectExpression.constant(MATCH_SIMPLE))
            .put(Columns.CONISLOCAL, () -> NestableCollectExpression.constant(true))
            .put(Columns.CONINHCOUNT, () -> NestableCollectExpression.constant(0))
            .put(Columns.CONNOINHERIT, () -> NestableCollectExpression.constant(true))
            .put(Columns.CONKEY, () -> NestableCollectExpression.constant(null))
            .put(Columns.CONFKEY, () -> NestableCollectExpression.constant(null))
            .put(Columns.CONPFEQOP, () -> NestableCollectExpression.constant(null))
            .put(Columns.CONPPEQOP, () -> NestableCollectExpression.constant(null))
            .put(Columns.CONFFEQOP, () -> NestableCollectExpression.constant(null))
            .put(Columns.CONEXCLOP, () -> NestableCollectExpression.constant(null))
            .put(Columns.CONBIN, () -> NestableCollectExpression.constant(null))
            .put(Columns.CONSRC, () -> NestableCollectExpression.constant(null))
            .build();
    }

    PgConstraintTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.OID.name(), DataTypes.INTEGER, null)
                .register(Columns.CONNAME.name(), DataTypes.STRING, null)
                .register(Columns.CONNAMESPACE.name(), DataTypes.INTEGER, null)
                .register(Columns.CONTYPE.name(), DataTypes.STRING, null)
                .register(Columns.CONDEFERRABLE.name(), DataTypes.BOOLEAN, null)
                .register(Columns.CONDEFERRED.name(), DataTypes.BOOLEAN, null)
                .register(Columns.CONVALIDATED.name(), DataTypes.BOOLEAN, null)
                .register(Columns.CONRELID.name(), DataTypes.INTEGER, null)
                .register(Columns.CONTYPID.name(), DataTypes.INTEGER, null)
                .register(Columns.CONINDID.name(), DataTypes.INTEGER, null)
                .register(Columns.CONFRELID.name(), DataTypes.INTEGER, null)
                .register(Columns.CONFUPDTYPE.name(), DataTypes.STRING, null)
                .register(Columns.CONFDELTYPE.name(), DataTypes.STRING, null)
                .register(Columns.CONFMATCHTYPE.name(), DataTypes.STRING, null)
                .register(Columns.CONISLOCAL.name(), DataTypes.BOOLEAN, null)
                .register(Columns.CONINHCOUNT.name(), DataTypes.INTEGER, null)
                .register(Columns.CONNOINHERIT.name(), DataTypes.BOOLEAN, null)
                .register(Columns.CONKEY.name(), DataTypes.SHORT_ARRAY, null)
                .register(Columns.CONFKEY.name(), DataTypes.SHORT_ARRAY, null)
                .register(Columns.CONPFEQOP.name(), DataTypes.INTEGER_ARRAY, null)
                .register(Columns.CONPPEQOP.name(), DataTypes.INTEGER_ARRAY, null)
                .register(Columns.CONFFEQOP.name(), DataTypes.INTEGER_ARRAY, null)
                .register(Columns.CONEXCLOP.name(), DataTypes.INTEGER_ARRAY, null)
                .register(Columns.CONBIN.name(), DataTypes.OBJECT, null)
                .register(Columns.CONSRC.name(), DataTypes.STRING, null),
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
