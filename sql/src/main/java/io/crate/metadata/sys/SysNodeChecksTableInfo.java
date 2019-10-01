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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.expression.reference.sys.check.node.SysNodeCheck;
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
import org.elasticsearch.cluster.ClusterState;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;


public class SysNodeChecksTableInfo extends StaticTableInfo<SysNodeCheck> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "node_checks");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private static final Set<Operation> SUPPORTED_OPERATIONS = EnumSet.of(Operation.READ, Operation.UPDATE);

    public static class Columns {
        public static final ColumnIdent ACKNOWLEDGED = new ColumnIdent("acknowledged");
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<SysNodeCheck>> expressions() {
        return columnRegistrar().expressions();
    }

    private static ColumnRegistrar<SysNodeCheck> columnRegistrar() {
        return new ColumnRegistrar<SysNodeCheck>(IDENT, GRANULARITY)
            .register("id", INTEGER, () -> forFunction(SysNodeCheck::id))
            .register("node_id", STRING, () -> forFunction(SysNodeCheck::nodeId))
            .register("severity", INTEGER, () -> forFunction((SysNodeCheck x) -> x.severity().value()))
            .register("description", STRING, () -> forFunction(SysNodeCheck::description))
            .register("passed", BOOLEAN, () -> forFunction(SysNodeCheck::isValid))
            .register("acknowledged", BOOLEAN, () -> forFunction(SysNodeCheck::acknowledged))
            .register(DocSysColumns.ID.name(), STRING, () -> forFunction(SysNodeCheck::rowId))
            .putInfoOnly(DocSysColumns.ID, DocSysColumns.forTable(IDENT, DocSysColumns.ID));
    }

    SysNodeChecksTableInfo() {
        super(IDENT, columnRegistrar(), "id", "node_id");
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
