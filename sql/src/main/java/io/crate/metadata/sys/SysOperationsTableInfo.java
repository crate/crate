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
import io.crate.planner.RowGranularity;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.*;

public class SysOperationsTableInfo extends SysTableInfo {

    public static class ColumnNames {
        public final static String ID = "id";
        public final static String JOB_ID = "job_id";
        public final static String NAME = "name";
        public final static String STARTED = "started";
        public final static String USED_BYTES = "used_bytes";
    }

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "operations");
    private static final String[] INDICES = new String[] { IDENT.name() };

    private static final Map<ColumnIdent, ReferenceInfo> COLUMNS_INFO = new LinkedHashMap<>();
    private static final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>();

    private static ReferenceInfo register(String column, DataType type) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(IDENT, column), RowGranularity.DOC, type);
        columns.add(info);
        COLUMNS_INFO.put(info.ident().columnIdent(), info);
        return info;
    }

    static {
        register(ColumnNames.ID, DataTypes.STRING);
        register(ColumnNames.JOB_ID, DataTypes.STRING);
        register(ColumnNames.NAME, DataTypes.STRING);
        register(ColumnNames.STARTED, DataTypes.TIMESTAMP);
        register(ColumnNames.USED_BYTES, DataTypes.LONG);
    }

    @Inject
    public SysOperationsTableInfo(ClusterService clusterService, SysSchemaInfo sysSchemaInfo) {
        super(clusterService, sysSchemaInfo);
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return columnInfo(columnIdent);
    }

    @Nullable
    public static ReferenceInfo columnInfo(ColumnIdent ident) {
        return COLUMNS_INFO.get(ident);
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
    public String[] concreteIndices() {
        return INDICES;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return COLUMNS_INFO.values().iterator();
    }
}
