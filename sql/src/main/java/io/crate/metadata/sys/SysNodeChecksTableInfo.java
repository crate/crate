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

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.expression.reference.sys.check.node.SysNodeCheck;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.EnumSet;
import java.util.Set;

public class SysNodeChecksTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "node_checks");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEYS = ImmutableList.of(Columns.ID, Columns.NODE_ID);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private static final Set<Operation> SUPPORTED_OPERATIONS = EnumSet.of(Operation.READ, Operation.UPDATE);

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        static final ColumnIdent NODE_ID = new ColumnIdent("node_id");
        static final ColumnIdent SEVERITY = new ColumnIdent("severity");
        public static final ColumnIdent DESCRIPTION = new ColumnIdent("description");
        static final ColumnIdent PASSED = new ColumnIdent("passed");
        public static final ColumnIdent ACKNOWLEDGED = new ColumnIdent("acknowledged");
    }

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysNodeCheck>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysNodeCheck>>builder()
            .put(SysNodeChecksTableInfo.Columns.ID,
                () -> NestableCollectExpression.forFunction(SysNodeCheck::id))
            .put(SysNodeChecksTableInfo.Columns.NODE_ID,
                () -> NestableCollectExpression.forFunction(SysNodeCheck::nodeId))
            .put(SysNodeChecksTableInfo.Columns.DESCRIPTION,
                () -> NestableCollectExpression.forFunction(SysNodeCheck::description))
            .put(SysNodeChecksTableInfo.Columns.SEVERITY,
                () -> NestableCollectExpression.forFunction((SysNodeCheck x) -> x.severity().value()))
            .put(SysNodeChecksTableInfo.Columns.PASSED,
                () -> NestableCollectExpression.forFunction(SysNodeCheck::isValid))
            .put(SysNodeChecksTableInfo.Columns.ACKNOWLEDGED,
                () -> NestableCollectExpression.forFunction(SysNodeCheck::acknowledged))
            .put(DocSysColumns.ID,
                () -> NestableCollectExpression.forFunction(SysNodeCheck::rowId))
            .build();
    }

    SysNodeChecksTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(SysNodeChecksTableInfo.Columns.ID, DataTypes.INTEGER)
                .register(SysNodeChecksTableInfo.Columns.NODE_ID, DataTypes.STRING)
                .register(SysNodeChecksTableInfo.Columns.SEVERITY, DataTypes.INTEGER)
                .register(SysNodeChecksTableInfo.Columns.DESCRIPTION, DataTypes.STRING)
                .register(SysNodeChecksTableInfo.Columns.PASSED, DataTypes.BOOLEAN)
                .register(SysNodeChecksTableInfo.Columns.ACKNOWLEDGED, DataTypes.BOOLEAN)
                .putInfoOnly(DocSysColumns.ID, DocSysColumns.forTable(IDENT, DocSysColumns.ID)),
            PRIMARY_KEYS);
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(ClusterState clusterState,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnAllNodes(IDENT, clusterState.getNodes());
    }

    @Override
    public Set<Operation> supportedOperations() {
        return SUPPORTED_OPERATIONS;
    }
}
