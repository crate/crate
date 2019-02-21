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
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

import static io.crate.metadata.pgcatalog.OidHash.relationOid;
import static io.crate.types.DataTypes.isCollectionType;

public class PgAttributeTable extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_attribute");

    static class Columns {
        static final ColumnIdent ATTRELID = new ColumnIdent("attrelid");
        static final ColumnIdent ATTNAME = new ColumnIdent("attname");
        static final ColumnIdent ATTTYPID = new ColumnIdent("atttypid");
        static final ColumnIdent ATTSTATTARGET = new ColumnIdent("attstattarget");
        static final ColumnIdent ATTLEN = new ColumnIdent("attlen");
        static final ColumnIdent ATTNUM = new ColumnIdent("attnum");
        static final ColumnIdent ATTNDIMS = new ColumnIdent("attndims");
        static final ColumnIdent ATTCACHEOFF = new ColumnIdent("attcacheoff");
        static final ColumnIdent ATTTYPMOD = new ColumnIdent("atttypmod");
        static final ColumnIdent ATTBYVAL = new ColumnIdent("attbyval");
        static final ColumnIdent ATTSTORAGE = new ColumnIdent("attstorage");
        static final ColumnIdent ATTALIGN = new ColumnIdent("attalign");
        static final ColumnIdent ATTNOTNULL = new ColumnIdent("attnotnull");
        static final ColumnIdent ATTHASDEF = new ColumnIdent("atthasdef");
        static final ColumnIdent ATTIDENTITY = new ColumnIdent("attidentity");
        static final ColumnIdent ATTISDROPPED = new ColumnIdent("attisdropped");
        static final ColumnIdent ATTISLOCAL = new ColumnIdent("attislocal");
        static final ColumnIdent ATTINHCOUNT = new ColumnIdent("attinhcount");
        static final ColumnIdent ATTCOLLATION = new ColumnIdent("attcollation");
        static final ColumnIdent ATTACL = new ColumnIdent("attacl");
        static final ColumnIdent ATTOPTIONS = new ColumnIdent("attoptions");
        static final ColumnIdent ATTFDWOPTIONS = new ColumnIdent("attfdwoptions");
    }


    public static Map<ColumnIdent, RowCollectExpressionFactory<ColumnContext>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ColumnContext>>builder()
            .put(Columns.ATTRELID, () -> NestableCollectExpression.forFunction(c -> relationOid(c.tableInfo)))
            .put(Columns.ATTNAME, () -> NestableCollectExpression.forFunction(c -> c.info.column().fqn()))
            .put(Columns.ATTTYPID, () -> NestableCollectExpression.forFunction(c -> PGTypes.get(c.info.valueType()).oid()))
            .put(Columns.ATTSTATTARGET, () -> NestableCollectExpression.constant(0))
            .put(Columns.ATTLEN, () -> NestableCollectExpression.forFunction(c -> PGTypes.get(c.info.valueType()).typeLen()))
            .put(Columns.ATTNUM, () -> NestableCollectExpression.forFunction(c -> c.ordinal))
            .put(Columns.ATTNDIMS, () -> NestableCollectExpression.forFunction(c -> isCollectionType(c.info.valueType()) ? 1 : 0))
            .put(Columns.ATTCACHEOFF, () -> NestableCollectExpression.constant(-1))
            .put(Columns.ATTTYPMOD, () -> NestableCollectExpression.constant(-1))
            .put(Columns.ATTBYVAL, () -> NestableCollectExpression.constant(false))
            .put(Columns.ATTSTORAGE, () -> NestableCollectExpression.constant(null))
            .put(Columns.ATTALIGN, () -> NestableCollectExpression.constant(null))
            .put(Columns.ATTNOTNULL, () -> NestableCollectExpression.forFunction(c -> !c.info.isNullable()))
            .put(Columns.ATTHASDEF, () -> NestableCollectExpression.constant(false)) // don't support default values
            .put(Columns.ATTIDENTITY, () -> NestableCollectExpression.constant(""))
            .put(Columns.ATTISDROPPED, () -> NestableCollectExpression.constant(false)) // don't support dropping columns
            .put(Columns.ATTISLOCAL, () -> NestableCollectExpression.constant(true))
            .put(Columns.ATTINHCOUNT, () -> NestableCollectExpression.constant(0))
            .put(Columns.ATTCOLLATION, () -> NestableCollectExpression.constant(0))
            .put(Columns.ATTACL, () -> NestableCollectExpression.constant(null))
            .put(Columns.ATTOPTIONS, () -> NestableCollectExpression.constant(null))
            .put(Columns.ATTFDWOPTIONS, () -> NestableCollectExpression.constant(null))
            .build();
    }

    PgAttributeTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.ATTRELID.name(), DataTypes.INTEGER)
                .register(Columns.ATTNAME.name(), DataTypes.STRING)
                .register(Columns.ATTTYPID.name(), DataTypes.INTEGER)
                .register(Columns.ATTSTATTARGET.name(), DataTypes.INTEGER)
                .register(Columns.ATTLEN.name(), DataTypes.INTEGER)
                .register(Columns.ATTNUM.name(), DataTypes.SHORT)
                .register(Columns.ATTNDIMS.name(), DataTypes.INTEGER)
                .register(Columns.ATTCACHEOFF.name(), DataTypes.INTEGER)
                .register(Columns.ATTTYPMOD.name(), DataTypes.INTEGER)
                .register(Columns.ATTBYVAL.name(), DataTypes.BOOLEAN)
                .register(Columns.ATTSTORAGE.name(), DataTypes.STRING)
                .register(Columns.ATTALIGN.name(), DataTypes.STRING)
                .register(Columns.ATTNOTNULL.name(), DataTypes.BOOLEAN)
                .register(Columns.ATTHASDEF.name(), DataTypes.BOOLEAN)
                .register(Columns.ATTIDENTITY.name(), DataTypes.STRING)
                .register(Columns.ATTISDROPPED.name(), DataTypes.BOOLEAN)
                .register(Columns.ATTISLOCAL.name(), DataTypes.BOOLEAN)
                .register(Columns.ATTINHCOUNT.name(), DataTypes.INTEGER)
                .register(Columns.ATTCOLLATION.name(), DataTypes.INTEGER)
                .register(Columns.ATTACL.name(), new ArrayType(ObjectType.untyped()))
                .register(Columns.ATTOPTIONS.name(), DataTypes.STRING_ARRAY)
                .register(Columns.ATTFDWOPTIONS.name(), DataTypes.STRING_ARRAY),
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
