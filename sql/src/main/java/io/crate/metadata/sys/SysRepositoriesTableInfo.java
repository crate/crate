/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;

import javax.annotation.Nullable;
import java.util.*;

public class SysRepositoriesTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "repositories");

    public static class Columns {
        public static final ColumnIdent NAME = new ColumnIdent("name");
        public static final ColumnIdent TYPE = new ColumnIdent("type");
        public static final ColumnIdent SETTINGS = new ColumnIdent("settings");
    }

    public static final Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>();

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(Columns.NAME);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;
    private static final LinkedHashSet<ReferenceInfo> COLUMNS = new LinkedHashSet<>();

    static {
        register(Columns.NAME, DataTypes.STRING);
        register(Columns.TYPE, DataTypes.STRING);
        register(Columns.SETTINGS, DataTypes.OBJECT);
    }

    private static ReferenceInfo register(ColumnIdent columnIdent, DataType type) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(IDENT, columnIdent), GRANULARITY, type);
        if (info.ident().isColumn()) {
            COLUMNS.add(info);
        }
        INFOS.put(info.ident().columnIdent(), info);
        return info;
    }

    public SysRepositoriesTableInfo(ClusterService clusterService, SysSchemaInfo sysSchemaInfo) {
        super(clusterService, sysSchemaInfo);
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return COLUMNS;
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
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        Map<String, List<Integer>> tableLocation = new TreeMap<>();
        tableLocation.put(IDENT.fqn(), null);
        locations.put(clusterService.localNode().id(), tableLocation);
        return new Routing(locations);
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return PRIMARY_KEY;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return INFOS.values().iterator();
    }
}
