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

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.List;

@Singleton
public class SysJobsLogTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "jobs_log");
    private final ClusterService clusterService;

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        public static final ColumnIdent STMT = new ColumnIdent("stmt");
        public static final ColumnIdent STARTED = new ColumnIdent("started");
        public static final ColumnIdent ENDED = new ColumnIdent("ended");
        public static final ColumnIdent ERROR = new ColumnIdent("error");
    }

    private final static List<ColumnIdent> primaryKeys = ImmutableList.of(Columns.ID);

    @Inject
    public SysJobsLogTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.ID, DataTypes.STRING)
            .register(Columns.STMT, DataTypes.STRING)
            .register(Columns.STARTED, DataTypes.TIMESTAMP)
            .register(Columns.ENDED, DataTypes.TIMESTAMP)
            .register(Columns.ERROR, DataTypes.STRING), primaryKeys);
        this.clusterService = clusterService;
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
