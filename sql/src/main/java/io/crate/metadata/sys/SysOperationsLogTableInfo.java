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
public class SysOperationsLogTableInfo extends SysTableInfo {

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        public static final ColumnIdent JOB_ID = new ColumnIdent("job_id");
        public static final ColumnIdent NAME = new ColumnIdent("name");
        public static final ColumnIdent STARTED = new ColumnIdent("started");
        public static final ColumnIdent ENDED = new ColumnIdent("ended");
        public static final ColumnIdent USED_BYTES = new ColumnIdent("used_bytes");
        public static final ColumnIdent ERROR = new ColumnIdent("error");
    }

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "operations_log");

    private final Map<ColumnIdent, ReferenceInfo> columns_info;
    private final Set<ReferenceInfo> columns;

    @Inject
    protected SysOperationsLogTableInfo(ClusterService clusterService, SysSchemaInfo sysSchemaInfo) {
        super(clusterService, sysSchemaInfo);
        ColumnRegistrar registrar = new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.ID, DataTypes.STRING)
            .register(Columns.JOB_ID, DataTypes.STRING)
            .register(Columns.NAME, DataTypes.STRING)
            .register(Columns.STARTED, DataTypes.TIMESTAMP)
            .register(Columns.ENDED, DataTypes.TIMESTAMP)
            .register(Columns.USED_BYTES, DataTypes.LONG)
            .register(Columns.ERROR, DataTypes.STRING);
        columns = registrar.columns();
        columns_info = registrar.infos();
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return columns_info.get(columnIdent);
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
        return columns_info.values().iterator();
    }
}
