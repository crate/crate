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
import io.crate.analyze.WhereClause;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.*;


@Singleton
public class SysChecksTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "checks");
    private static final ImmutableList<ColumnIdent> primaryKeys = ImmutableList.of(new ColumnIdent("id"));
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private final Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>();
    private final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>();

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        public static final ColumnIdent SEVERITY = new ColumnIdent("severity");
        public static final ColumnIdent DESCRIPTION = new ColumnIdent("description");
        public static final ColumnIdent PASSED = new ColumnIdent("passed");

    }

    private ReferenceInfo register(ColumnIdent columnIdent, DataType type) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(IDENT, columnIdent), GRANULARITY, type);
        if (info.ident().isColumn()) {
            columns.add(info);
        }
        INFOS.put(info.ident().columnIdent(), info);
        return info;
    }

    @Inject
    protected SysChecksTableInfo(ClusterService clusterService, SysSchemaInfo sysSchemaInfo) {
        super(clusterService, sysSchemaInfo);
        register(Columns.ID, DataTypes.INTEGER);
        register(Columns.SEVERITY, DataTypes.INTEGER);
        register(Columns.DESCRIPTION, DataTypes.STRING);
        register(Columns.PASSED, DataTypes.BOOLEAN);
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return new Routing(
                TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder().put(
                        clusterService.localNode().id(),
                        TreeMapBuilder.<String, List<Integer>>newMapBuilder().put(IDENT.fqn(), null).map()).map()
        );
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKeys;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return INFOS.values().iterator();
    }
}
