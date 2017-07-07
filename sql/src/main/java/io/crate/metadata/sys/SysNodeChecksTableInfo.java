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
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.operation.reference.sys.check.node.SysNodeCheck;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.Set;

public class SysNodeChecksTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "node_checks");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEYS = ImmutableList.of(Columns.ID, Columns.NODE_ID);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private final ClusterService clusterService;
    private final static Set<Operation> SUPPORTED_OPERATIONS = EnumSet.of(Operation.READ, Operation.UPDATE);

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
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::id))
            .put(SysNodeChecksTableInfo.Columns.NODE_ID,
                () -> RowContextCollectorExpression.objToBytesRef(SysNodeCheck::nodeId))
            .put(SysNodeChecksTableInfo.Columns.DESCRIPTION,
                () -> RowContextCollectorExpression.objToBytesRef(SysNodeCheck::description))
            .put(SysNodeChecksTableInfo.Columns.SEVERITY,
                () -> RowContextCollectorExpression.forFunction((SysNodeCheck x) -> x.severity().value()))
            .put(SysNodeChecksTableInfo.Columns.PASSED,
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::validate))
            .put(SysNodeChecksTableInfo.Columns.ACKNOWLEDGED,
                () -> RowContextCollectorExpression.forFunction(SysNodeCheck::acknowledged))
            .put(DocSysColumns.ID,
                () -> RowContextCollectorExpression.objToBytesRef(SysNodeCheck::rowId))
            .build();
    }

    SysNodeChecksTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(SysNodeChecksTableInfo.Columns.ID, DataTypes.INTEGER)
                .register(SysNodeChecksTableInfo.Columns.NODE_ID, DataTypes.STRING)
                .register(SysNodeChecksTableInfo.Columns.SEVERITY, DataTypes.INTEGER)
                .register(SysNodeChecksTableInfo.Columns.DESCRIPTION, DataTypes.STRING)
                .register(SysNodeChecksTableInfo.Columns.PASSED, DataTypes.BOOLEAN)
                .register(SysNodeChecksTableInfo.Columns.ACKNOWLEDGED, DataTypes.BOOLEAN)
                .putInfoOnly(DocSysColumns.ID, DocSysColumns.forTable(IDENT, DocSysColumns.ID)),
            PRIMARY_KEYS);
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return Routing.forTableOnAllNodes(IDENT, clusterService.state().nodes());
    }

    @Override
    public Set<Operation> supportedOperations() {
        return SUPPORTED_OPERATIONS;
    }
}
