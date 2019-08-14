/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.sys;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.expression.reference.sys.operation.OperationContextLog;
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

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.TIMESTAMPZ;

public class SysOperationsLogTableInfo extends StaticTableInfo<OperationContextLog> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "operations_log");

    public static Map<ColumnIdent, RowCollectExpressionFactory<OperationContextLog>> expressions() {
        return columnRegistrar().expressions();
    }

    private static ColumnRegistrar<OperationContextLog> columnRegistrar() {
        return new ColumnRegistrar<OperationContextLog>(IDENT, RowGranularity.DOC)
            .register("id", STRING, () -> forFunction(l -> String.valueOf(l.id())))
            .register("job_id", STRING, () -> forFunction(l -> l.jobId().toString()))
            .register("name", STRING, () -> forFunction(OperationContextLog::name))
            .register("started", TIMESTAMPZ, () -> forFunction(OperationContextLog::started))
            .register("ended", TIMESTAMPZ, () -> forFunction(OperationContextLog::ended))
            .register("used_bytes", LONG, () -> forFunction(OperationContextLog::usedBytes))
            .register("error", STRING, () -> forFunction(OperationContextLog::errorMessage));
    }

    SysOperationsLogTableInfo() {
        super(IDENT, columnRegistrar());
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public RelationName ident() {
        return IDENT;
    }

    @Override
    public Routing getRouting(ClusterState clusterState,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        return Routing.forTableOnAllNodes(IDENT, clusterState.getNodes());
    }
}
