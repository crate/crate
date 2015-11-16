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
import io.crate.metadata.*;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.*;

@Singleton
public class SysOperationsTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "operations");

    public static class Columns {
        public final static ColumnIdent ID = new ColumnIdent("id");
        public final static ColumnIdent JOB_ID = new ColumnIdent("job_id");
        public final static ColumnIdent NAME = new ColumnIdent("name");
        public final static ColumnIdent STARTED = new ColumnIdent("started");
        public final static ColumnIdent USED_BYTES = new ColumnIdent("used_bytes");
    }

    private final TableColumn nodesTableColumn;
    private final Map<ColumnIdent, ReferenceInfo> infos;
    private final Set<ReferenceInfo> columns;

    @Inject
    public SysOperationsTableInfo(ClusterService clusterService,
                                  SysSchemaInfo sysSchemaInfo,
                                  SysNodesTableInfo sysNodesTableInfo) {
        super(clusterService, sysSchemaInfo);
        nodesTableColumn = sysNodesTableInfo.tableColumn();
        ColumnRegistrar columnRegistrar = new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.ID, DataTypes.STRING)
            .register(Columns.JOB_ID, DataTypes.STRING)
            .register(Columns.NAME, DataTypes.STRING)
            .register(Columns.STARTED, DataTypes.TIMESTAMP)
            .register(Columns.USED_BYTES, DataTypes.LONG)
            .putInfoOnly(SysNodesTableInfo.SYS_COL_IDENT, SysNodesTableInfo.tableColumnInfo(IDENT));
        infos = columnRegistrar.infos();
        columns = columnRegistrar.columns();
    }


    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        ReferenceInfo info = infos.get(columnIdent);
        if (info == null) {
            return nodesTableColumn.getReferenceInfo(this.ident(), columnIdent);
        }
        return info;
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
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
        return tableRouting(whereClause);
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return ImmutableList.of();
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return infos.values().iterator();
    }
}
