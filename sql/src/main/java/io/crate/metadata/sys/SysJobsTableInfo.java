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

import javax.annotation.Nullable;

public class SysJobsTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "jobs");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(Columns.ID);
    private final ClusterService service;

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        public static final ColumnIdent STMT = new ColumnIdent("stmt");
        public static final ColumnIdent STARTED = new ColumnIdent("started");
    }

    @Inject
    public SysJobsTableInfo(ClusterService service) {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.ID, DataTypes.STRING)
            .register(Columns.STMT, DataTypes.STRING)
            .register(Columns.STARTED, DataTypes.TIMESTAMP), PRIMARY_KEY);
        this.service = service;
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
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return Routing.forTableOnAllNodes(IDENT, service.state().nodes());
    }
}
