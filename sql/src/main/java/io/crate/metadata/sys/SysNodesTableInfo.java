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
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import org.cratedb.DataType;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.*;

public class SysNodesTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "nodes");

    private static final ImmutableList<String> primaryKey = ImmutableList.of("id");

    public static final Map<ColumnIdent, ReferenceInfo> INFOS = new HashMap<>();
    private static final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>();

    static {
        register(getReferenceInfo(primaryKey.get(0), DataType.STRING, null));
        register(getReferenceInfo("name", DataType.STRING, null));
        register(getReferenceInfo("hostname", DataType.STRING, null));
        register(getReferenceInfo("port", DataType.OBJECT, null, Arrays.asList(
                getReferenceInfo("port", DataType.INTEGER, ImmutableList.of("http")),
                getReferenceInfo("port", DataType.INTEGER, ImmutableList.of("transport"))
        )));
        register(getReferenceInfo("load", DataType.OBJECT, null, Arrays.asList(
                getReferenceInfo("load", DataType.DOUBLE, ImmutableList.of("1")),
                getReferenceInfo("load", DataType.DOUBLE, ImmutableList.of("5")),
                getReferenceInfo("load", DataType.DOUBLE, ImmutableList.of("15"))
        )));
        register(getReferenceInfo("mem", DataType.OBJECT, null, Arrays.asList(
                getReferenceInfo("mem", DataType.LONG, ImmutableList.of("free")),
                getReferenceInfo("mem", DataType.LONG, ImmutableList.of("used")),
                getReferenceInfo("mem", DataType.SHORT, ImmutableList.of("free_percent")),
                getReferenceInfo("mem", DataType.SHORT, ImmutableList.of("used_percent"))
        )));

        register(getReferenceInfo("fs", DataType.OBJECT, null, Arrays.asList(
                getReferenceInfo("fs", DataType.LONG, ImmutableList.of("total")),
                getReferenceInfo("fs", DataType.LONG, ImmutableList.of("free")),
                getReferenceInfo("fs", DataType.LONG, ImmutableList.of("used")),
                getReferenceInfo("fs", DataType.LONG, ImmutableList.of("free_percent")),
                getReferenceInfo("fs", DataType.LONG, ImmutableList.of("used_percent"))
        )));
    }

    private final ClusterService clusterService;

    @Inject
    public SysNodesTableInfo(ClusterService service) {
        clusterService = service;
    }

    private static ReferenceInfo getReferenceInfo(String column, DataType type, List<String> path) {
        return getReferenceInfo(column, type, path, null);
    }

    private static ReferenceInfo getReferenceInfo(String column, DataType type, List<String> path, @Nullable List<ReferenceInfo> nestedColumns) {
        return new ReferenceInfo(new ReferenceIdent(IDENT, column, path), RowGranularity.NODE, type, nestedColumns);
    }

    private static void register(ReferenceInfo info) {
        if (info.ident().isColumn()) {
            columns.add(info);
        }
        INFOS.put(info.ident().columnIdent(), info);
        for (ReferenceInfo nestedColumn : info.nestedColumns()) {
            register(nestedColumn);
        }

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
    public RowGranularity rowGranularity() {
        return RowGranularity.NODE;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public Routing getRouting(Function whereClause) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        ImmutableMap.Builder<String, Map<String, Set<Integer>>> builder = ImmutableMap.builder();

        for (DiscoveryNode node : nodes) {
            builder.put(node.id(), ImmutableMap.<String, Set<Integer>>of());
        }

        return new Routing(builder.build());
    }

    @Override
    public List<String> primaryKey() {
        return primaryKey;
    }

    @Override
    public String clusteredBy() {
        return null;
    }
}
