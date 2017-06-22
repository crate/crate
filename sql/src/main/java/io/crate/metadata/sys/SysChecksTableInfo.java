/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

import com.google.common.collect.ImmutableList;
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
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;

public class SysChecksTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "checks");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEYS = ImmutableList.of(Columns.ID);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private final ClusterService clusterService;

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        static final ColumnIdent SEVERITY = new ColumnIdent("severity");
        public static final ColumnIdent DESCRIPTION = new ColumnIdent("description");
        static final ColumnIdent PASSED = new ColumnIdent("passed");
    }

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysCheck>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysCheck>>builder()
            .put(SysChecksTableInfo.Columns.ID,
                () -> RowContextCollectorExpression.forFunction(SysCheck::id))
            .put(SysChecksTableInfo.Columns.DESCRIPTION,
                () -> RowContextCollectorExpression.objToBytesRef(SysCheck::description))
            .put(SysChecksTableInfo.Columns.SEVERITY,
                () -> RowContextCollectorExpression.forFunction((SysCheck r) -> r.severity().value()))
            .put(SysChecksTableInfo.Columns.PASSED,
                () -> RowContextCollectorExpression.forFunction(SysCheck::validate))
            .build();
    }

    SysChecksTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.ID, DataTypes.INTEGER)
                .register(Columns.SEVERITY, DataTypes.INTEGER)
                .register(Columns.DESCRIPTION, DataTypes.STRING)
                .register(Columns.PASSED, DataTypes.BOOLEAN),
            PRIMARY_KEYS);
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference, SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, clusterService.localNode().getId());
    }
}
