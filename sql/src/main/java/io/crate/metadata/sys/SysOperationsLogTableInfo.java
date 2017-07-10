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
import io.crate.metadata.Routing;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.operation.reference.sys.operation.OperationContextLog;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class SysOperationsLogTableInfo extends StaticTableInfo {

    private final ClusterService clusterService;

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        static final ColumnIdent JOB_ID = new ColumnIdent("job_id");
        public static final ColumnIdent NAME = new ColumnIdent("name");
        public static final ColumnIdent STARTED = new ColumnIdent("started");
        static final ColumnIdent ENDED = new ColumnIdent("ended");
        static final ColumnIdent USED_BYTES = new ColumnIdent("used_bytes");
        public static final ColumnIdent ERROR = new ColumnIdent("error");
    }

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "operations_log");

    public static Map<ColumnIdent, RowCollectExpressionFactory<OperationContextLog>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<OperationContextLog>>builder()
            .put(SysOperationsLogTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContextLog::id))
            .put(SysOperationsLogTableInfo.Columns.JOB_ID,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContextLog::jobId))
            .put(SysOperationsLogTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContextLog::name))
            .put(SysOperationsLogTableInfo.Columns.STARTED,
                () -> RowContextCollectorExpression.forFunction(OperationContextLog::started))
            .put(SysOperationsLogTableInfo.Columns.USED_BYTES, () -> new RowContextCollectorExpression<OperationContextLog, Long>() {
                @Override
                public Long value() {
                    long usedBytes = row.usedBytes();
                    if (usedBytes == 0) {
                        return null;
                    }
                    return usedBytes;
                }
            })
            .put(SysOperationsLogTableInfo.Columns.ERROR,
                () -> RowContextCollectorExpression.objToBytesRef(OperationContextLog::errorMessage))
            .put(SysOperationsLogTableInfo.Columns.ENDED,
                () -> RowContextCollectorExpression.forFunction(OperationContextLog::ended))
            .build();
    }

    SysOperationsLogTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.ID, DataTypes.STRING)
            .register(Columns.JOB_ID, DataTypes.STRING)
            .register(Columns.NAME, DataTypes.STRING)
            .register(Columns.STARTED, DataTypes.TIMESTAMP)
            .register(Columns.ENDED, DataTypes.TIMESTAMP)
            .register(Columns.USED_BYTES, DataTypes.LONG)
            .register(Columns.ERROR, DataTypes.STRING), Collections.emptyList());
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference, SessionContext sessionContext) {
        return Routing.forTableOnAllNodes(IDENT, clusterService.state().nodes());
    }
}
