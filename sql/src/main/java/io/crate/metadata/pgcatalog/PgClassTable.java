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
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

import static io.crate.metadata.pgcatalog.OidHash.schemaOid;

public class PgClassTable extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_class");

    static class Columns {
        static final ColumnIdent OID = new ColumnIdent("oid");
        static final ColumnIdent RELNAME = new ColumnIdent("relname");
        static final ColumnIdent RELNAMESPACE = new ColumnIdent("relnamespace");
        static final ColumnIdent RELTYPE = new ColumnIdent("reltype");
        static final ColumnIdent RELOFTYPE = new ColumnIdent("reloftype");
        static final ColumnIdent RELOWNER = new ColumnIdent("relowner");
        static final ColumnIdent RELAM = new ColumnIdent("relam");
        static final ColumnIdent RELFILENODE = new ColumnIdent("relfilenode");
        static final ColumnIdent RELTABLESPACE = new ColumnIdent("reltablespace");
        static final ColumnIdent RELPAGES = new ColumnIdent("relpages");
        static final ColumnIdent RELTUPLES = new ColumnIdent("reltuples");
        static final ColumnIdent RELALLVISIBLE = new ColumnIdent("relallvisible");
        static final ColumnIdent RELTOASTRELID = new ColumnIdent("reltoastrelid");
        static final ColumnIdent RELHASINDEX = new ColumnIdent("relhasindex");
        static final ColumnIdent RELISSHARED = new ColumnIdent("relisshared");
        static final ColumnIdent RELPERSISTENCE = new ColumnIdent("relpersistence");
        static final ColumnIdent RELKIND = new ColumnIdent("relkind");
        static final ColumnIdent RELNATTS = new ColumnIdent("relnatts");
        static final ColumnIdent RELCHECKS = new ColumnIdent("relchecks");
        static final ColumnIdent RELHASOIDS = new ColumnIdent("relhasoids");
        static final ColumnIdent RELHASPKEY = new ColumnIdent("relhaspkey");
        static final ColumnIdent RELHASRULES = new ColumnIdent("relhasrules");
        static final ColumnIdent RELHASTRIGGERS = new ColumnIdent("relhastriggers");
        static final ColumnIdent RELHASSUBCLASS = new ColumnIdent("relhassubclass");
        static final ColumnIdent RELROWSECURITY = new ColumnIdent("relrowsecurity");
        static final ColumnIdent RELFORCEROWSECURITY = new ColumnIdent("relforcerowsecurity");
        static final ColumnIdent RELISPOPULATED = new ColumnIdent("relispopulated");
        static final ColumnIdent RELREPLIDENT = new ColumnIdent("relreplident");
        static final ColumnIdent RELISPARTITION = new ColumnIdent("relispartition");
        static final ColumnIdent RELFROZENXID = new ColumnIdent("relfrozenxid");
        static final ColumnIdent RELMINMXID = new ColumnIdent("relminmxid");
        static final ColumnIdent RELACL = new ColumnIdent("relacl");
        static final ColumnIdent RELOPTIONS = new ColumnIdent("reloptions");
        static final ColumnIdent RELPARTBOUND = new ColumnIdent("relpartbound");
    }

    private static final String KIND_TABLE = "r";
    private static final String KIND_VIEW = "v";

    private static final String PERSISTENCE_PERMANENT = "p";

    public static Map<ColumnIdent, RowCollectExpressionFactory<RelationInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<RelationInfo>>builder()
            .put(Columns.OID, () -> NestableCollectExpression.forFunction(OidHash::relationOid))
            .put(Columns.RELNAME, () -> NestableCollectExpression.forFunction(r -> r.ident().name()))
            .put(Columns.RELNAMESPACE, () -> NestableCollectExpression.forFunction(r -> schemaOid(r.ident().schema())))
            .put(Columns.RELTYPE, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELOFTYPE, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELOWNER, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELAM, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELFILENODE, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELTABLESPACE, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELPAGES, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELTUPLES, () -> NestableCollectExpression.constant(0.0f))
            .put(Columns.RELALLVISIBLE, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELTOASTRELID, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELHASINDEX, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELISSHARED, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELPERSISTENCE, () -> NestableCollectExpression.constant(PERSISTENCE_PERMANENT))
            .put(Columns.RELKIND, () -> NestableCollectExpression.forFunction(
                r -> r.relationType() == RelationType.VIEW ? KIND_VIEW : KIND_TABLE))
            .put(Columns.RELNATTS, () -> NestableCollectExpression.forFunction(r -> (short) r.columns().size()))
            .put(Columns.RELCHECKS, () -> NestableCollectExpression.constant((short) 0))
            .put(Columns.RELHASOIDS, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELHASPKEY, () -> NestableCollectExpression.forFunction(r -> r.primaryKey().size() > 0))
            .put(Columns.RELHASRULES, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELHASTRIGGERS, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELHASSUBCLASS, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELROWSECURITY, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELFORCEROWSECURITY, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELISPOPULATED, () -> NestableCollectExpression.constant(true))
            .put(Columns.RELREPLIDENT, () -> NestableCollectExpression.constant("p"))
            .put(Columns.RELISPARTITION, () -> NestableCollectExpression.constant(false))
            .put(Columns.RELFROZENXID, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELMINMXID, () -> NestableCollectExpression.constant(0))
            .put(Columns.RELACL, () -> NestableCollectExpression.constant(null))
            .put(Columns.RELOPTIONS, () -> NestableCollectExpression.constant(null))
            .put(Columns.RELPARTBOUND, () -> NestableCollectExpression.constant(null))
            .build();
    }

    PgClassTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.OID.name(), DataTypes.INTEGER, null)
                .register(Columns.RELNAME.name(), DataTypes.STRING, null)
                .register(Columns.RELNAMESPACE.name(), DataTypes.INTEGER, null)
                .register(Columns.RELTYPE.name(), DataTypes.INTEGER, null)
                .register(Columns.RELOFTYPE.name(), DataTypes.INTEGER, null)
                .register(Columns.RELOWNER.name(), DataTypes.INTEGER, null)
                .register(Columns.RELAM.name(), DataTypes.INTEGER, null)
                .register(Columns.RELFILENODE.name(), DataTypes.INTEGER, null)
                .register(Columns.RELTABLESPACE.name(), DataTypes.INTEGER, null)
                .register(Columns.RELPAGES.name(), DataTypes.INTEGER, null)
                .register(Columns.RELTUPLES.name(), DataTypes.FLOAT, null)
                .register(Columns.RELALLVISIBLE.name(), DataTypes.INTEGER, null)
                .register(Columns.RELTOASTRELID.name(), DataTypes.INTEGER, null)
                .register(Columns.RELHASINDEX.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELISSHARED.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELPERSISTENCE.name(), DataTypes.STRING, null)
                .register(Columns.RELKIND.name(), DataTypes.STRING, null)
                .register(Columns.RELNATTS.name(), DataTypes.SHORT, null)
                .register(Columns.RELCHECKS.name(), DataTypes.SHORT, null)
                .register(Columns.RELHASOIDS.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELHASPKEY.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELHASRULES.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELHASTRIGGERS.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELHASSUBCLASS.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELROWSECURITY.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELFORCEROWSECURITY.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELISPOPULATED.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELREPLIDENT.name(), DataTypes.STRING, null)
                .register(Columns.RELISPARTITION.name(), DataTypes.BOOLEAN, null)
                .register(Columns.RELFROZENXID.name(), DataTypes.INTEGER, null)
                .register(Columns.RELMINMXID.name(), DataTypes.INTEGER, null)
                .register(Columns.RELACL.name(), DataTypes.OBJECT_ARRAY, null)
                .register(Columns.RELOPTIONS.name(), DataTypes.STRING_ARRAY, null)
                .register(Columns.RELPARTBOUND.name(), DataTypes.OBJECT, null),
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
