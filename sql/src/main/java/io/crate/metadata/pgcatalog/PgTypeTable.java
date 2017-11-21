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
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.protocols.postgres.types.PGType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterState;

import java.util.Collections;
import java.util.Map;

public class PgTypeTable extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(PgCatalogSchemaInfo.NAME, "pg_type");

    static class Columns {
        static final ColumnIdent OID = new ColumnIdent("oid");
        static final ColumnIdent TYPNAME = new ColumnIdent("typname");
        static final ColumnIdent TYPDELIM = new ColumnIdent("typdelim");
        static final ColumnIdent TYPELEM = new ColumnIdent("typelem");
        static final ColumnIdent TYPTYPE = new ColumnIdent("typtype");
    }

    private static final BytesRef TYPTYPE_BASE = new BytesRef("b");

    public static Map<ColumnIdent, RowCollectExpressionFactory<PGType>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<PGType>>builder()
            .put(Columns.OID,
                () -> RowContextCollectorExpression.forFunction(PGType::oid))
            .put(Columns.TYPNAME,
                () -> RowContextCollectorExpression.objToBytesRef(PGType::typName))
            .put(Columns.TYPDELIM,
                () -> RowContextCollectorExpression.objToBytesRef(PGType::typDelim))
            .put(Columns.TYPELEM,
                () -> RowContextCollectorExpression.forFunction(PGType::typElem))
            .put(Columns.TYPTYPE,
                () -> new RowContextCollectorExpression<PGType, BytesRef>() {

                    @Override
                    public BytesRef value() {
                        return TYPTYPE_BASE;
                    }
                })
            .build();
    }

    PgTypeTable() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.OID.name(), DataTypes.INTEGER, null)
                .register(Columns.TYPNAME.name(), DataTypes.STRING, null)
                .register(Columns.TYPDELIM.name(), DataTypes.STRING, null)
                .register(Columns.TYPELEM.name(), DataTypes.INTEGER, null)
                .register(Columns.TYPTYPE.name(), DataTypes.STRING, null),
            Collections.emptyList());
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
