/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.cratedb.DataType;

import java.util.*;

public class SysClusterTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "cluster");
    private static final String[] PARTITIONS = new String[]{IDENT.name()};

    public static final Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>();
    private static final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>();

    static {
        register("id", DataType.STRING, null);
        register("name", DataType.STRING, null);
    }

    private static ReferenceInfo register(String column, DataType type, List<String> path) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(IDENT, column, path), RowGranularity.CLUSTER, type);
        if (info.ident().isColumn()) {
            columns.add(info);
        }
        INFOS.put(info.ident().columnIdent(), info);
        return info;
    }

    @Override
    public ReferenceInfo getColumnInfo(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public Routing getRouting(WhereClause whereClause) {
        // No routing for cluster level
        return null;
    }

    @Override
    public List<String> primaryKey() {
        return ImmutableList.of();
    }

    @Override
    public String clusteredBy() {
        return null;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.CLUSTER;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public String[] partitions() {
        return PARTITIONS;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return INFOS.values().iterator();
    }
}
