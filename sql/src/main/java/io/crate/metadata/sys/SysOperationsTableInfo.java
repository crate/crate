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

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.Routing;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.operation.reference.sys.operation.OperationContext;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class SysOperationsTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "operations");
    private final ClusterService clusterService;

    public static class Columns {
        public final static ColumnIdent ID = new ColumnIdent("id");
        final static ColumnIdent JOB_ID = new ColumnIdent("job_id");
        public final static ColumnIdent NAME = new ColumnIdent("name");
        public final static ColumnIdent STARTED = new ColumnIdent("started");
        final static ColumnIdent USED_BYTES = new ColumnIdent("used_bytes");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<OperationContext>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<OperationContext>>builder()
            .put(SysOperationsTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContext::id))
            .put(SysOperationsTableInfo.Columns.JOB_ID,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContext::jobId))
            .put(SysOperationsTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContext::name))
            .put(SysOperationsTableInfo.Columns.STARTED,
                () -> RowContextCollectorExpression.forFunction(OperationContext::started))
            .put(SysOperationsTableInfo.Columns.USED_BYTES, () -> RowContextCollectorExpression.forFunction(r -> {
                if (r.usedBytes == 0) {
                    return null;
                }
                return r.usedBytes;
            }))
            .build();
    }

    private final TableColumn nodesTableColumn;

    SysOperationsTableInfo(ClusterService clusterService, SysNodesTableInfo sysNodesTableInfo) {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.ID, DataTypes.STRING)
                .register(Columns.JOB_ID, DataTypes.STRING)
                .register(Columns.NAME, DataTypes.STRING)
                .register(Columns.STARTED, DataTypes.TIMESTAMP)
                .register(Columns.USED_BYTES, DataTypes.LONG)
                .putInfoOnly(SysNodesTableInfo.SYS_COL_IDENT, SysNodesTableInfo.tableColumnInfo(IDENT)),
            Collections.<ColumnIdent>emptyList());
        this.clusterService = clusterService;
        nodesTableColumn = sysNodesTableInfo.tableColumn();
    }


    @Nullable
    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        Reference info = super.getReference(columnIdent);
        if (info == null) {
            return nodesTableColumn.getReference(this.ident(), columnIdent);
        }
        return info;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference, SessionContext sessionContext) {
        return Routing.forTableOnAllNodes(IDENT, clusterService.state().nodes());
    }
}
