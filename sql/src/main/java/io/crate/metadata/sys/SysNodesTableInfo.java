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
import io.crate.analyze.where.WhereClause;
import io.crate.metadata.*;
import io.crate.planner.RowGranularity;
import io.crate.types.*;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;

import java.util.*;

public class SysNodesTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "nodes");
    private static final String[] PARTITIONS = new String[]{IDENT.name()};

    private static final ImmutableList<ColumnIdent> primaryKey = ImmutableList.of(
            new ColumnIdent("id"));

    public static final Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>();
    private static final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>();

    static {
        DataType objectArrayType = new ArrayType(DataTypes.OBJECT);

        register("id", DataTypes.STRING, null);
        register("name", DataTypes.STRING, null);
        register("hostname", DataTypes.STRING, null);
        register("port", DataTypes.OBJECT, null);
        register("port", DataTypes.INTEGER, ImmutableList.of("http"));
        register("port", DataTypes.INTEGER, ImmutableList.of("transport"));
        register("load", DataTypes.OBJECT, null);
        register("load", DataTypes.DOUBLE, ImmutableList.of("1"));
        register("load", DataTypes.DOUBLE, ImmutableList.of("5"));
        register("load", DataTypes.DOUBLE, ImmutableList.of("15"));
        register("mem", DataTypes.OBJECT, null);
        register("mem", DataTypes.LONG, ImmutableList.of("free"));
        register("mem", DataTypes.LONG, ImmutableList.of("used"));
        register("mem", DataTypes.SHORT, ImmutableList.of("free_percent"));
        register("mem", DataTypes.SHORT, ImmutableList.of("used_percent"));
        register("heap", DataTypes.OBJECT, null);
        register("heap", DataTypes.LONG, ImmutableList.of("free"));
        register("heap", DataTypes.LONG, ImmutableList.of("used"));
        register("heap", DataTypes.LONG, ImmutableList.of("max"));
        register("version", DataTypes.OBJECT, null);
        register("version", StringType.INSTANCE, ImmutableList.of("number"));
        register("version", StringType.INSTANCE, ImmutableList.of("build_hash"));
        register("version", DataTypes.BOOLEAN, ImmutableList.of("build_snapshot"));
        register("thread_pools", objectArrayType, null);
        register("thread_pools", StringType.INSTANCE, ImmutableList.of("name"));
        register("thread_pools", IntegerType.INSTANCE, ImmutableList.of("active"));
        register("thread_pools", LongType.INSTANCE, ImmutableList.of("rejected"));
        register("thread_pools", IntegerType.INSTANCE, ImmutableList.of("largest"));
        register("thread_pools", LongType.INSTANCE, ImmutableList.of("completed"));
        register("thread_pools", IntegerType.INSTANCE, ImmutableList.of("threads"));
        register("thread_pools", IntegerType.INSTANCE, ImmutableList.of("queue"));

        register("network", DataTypes.OBJECT, null);
        register("network", DataTypes.OBJECT, ImmutableList.of("tcp"));
        register("network", DataTypes.OBJECT, ImmutableList.of("tcp", "connections"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "connections", "initiated"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "connections", "accepted"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "connections", "curr_established"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "connections", "dropped"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "connections", "embryonic_dropped"));
        register("network", DataTypes.OBJECT, ImmutableList.of("tcp", "packets"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "packets", "sent"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "packets", "received"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "packets", "retransmitted"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "packets", "errors_received"));
        register("network", DataTypes.LONG, ImmutableList.of("tcp", "packets", "rst_sent"));

        register("os", DataTypes.OBJECT, null);
        register("os", DataTypes.LONG, ImmutableList.of("uptime"));
        register("os", DataTypes.TIMESTAMP, ImmutableList.of("timestamp"));
        register("os", DataTypes.OBJECT, ImmutableList.of("cpu"));
        register("os", DataTypes.SHORT, ImmutableList.of("cpu", "system"));
        register("os", DataTypes.SHORT, ImmutableList.of("cpu", "user"));
        register("os", DataTypes.SHORT, ImmutableList.of("cpu", "idle"));
        register("os", DataTypes.SHORT, ImmutableList.of("cpu", "used"));
        register("os", DataTypes.SHORT, ImmutableList.of("cpu", "stolen"));
        register("process", DataTypes.OBJECT, null);
        register("process", DataTypes.LONG, ImmutableList.of("open_file_descriptors"));
        register("process", DataTypes.LONG, ImmutableList.of("max_open_file_descriptors"));

        register("fs", DataTypes.OBJECT, null);
        register("fs", DataTypes.OBJECT, ImmutableList.of("total"));
        register("fs", DataTypes.LONG, ImmutableList.of("total", "size"));
        register("fs", DataTypes.LONG, ImmutableList.of("total", "used"));
        register("fs", DataTypes.LONG, ImmutableList.of("total", "available"));
        register("fs", DataTypes.LONG, ImmutableList.of("total", "reads"));
        register("fs", DataTypes.LONG, ImmutableList.of("total", "bytes_read"));
        register("fs", DataTypes.LONG, ImmutableList.of("total", "writes"));
        register("fs", DataTypes.LONG, ImmutableList.of("total", "bytes_written"));

        register("fs", objectArrayType, ImmutableList.of("disks"));
        register("fs", DataTypes.STRING, ImmutableList.of("disks", "dev"));
        register("fs", DataTypes.LONG, ImmutableList.of("disks", "size"));
        register("fs", DataTypes.LONG, ImmutableList.of("disks", "used"));
        register("fs", DataTypes.LONG, ImmutableList.of("disks", "available"));
        register("fs", DataTypes.LONG, ImmutableList.of("disks", "reads"));
        register("fs", DataTypes.LONG, ImmutableList.of("disks", "bytes_read"));
        register("fs", DataTypes.LONG, ImmutableList.of("disks", "writes"));
        register("fs", DataTypes.LONG, ImmutableList.of("disks", "bytes_written"));

        register("fs", objectArrayType, ImmutableList.of("data"));
        register("fs", DataTypes.STRING, ImmutableList.of("data", "dev"));
        register("fs", DataTypes.STRING, ImmutableList.of("data", "path"));
    }

    @Inject
    public SysNodesTableInfo(ClusterService service, SysSchemaInfo sysSchemaInfo) {
        super(service, sysSchemaInfo);
    }

    private static ReferenceInfo register(String column, DataType type, List<String> path) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(IDENT, column, path), RowGranularity.NODE, type);
        if (info.ident().isColumn()) {
            columns.add(info);
        }
        INFOS.put(info.ident().columnIdent(), info);
        return info;
    }

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
        return RowGranularity.NODE;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }

    @Override
    public Routing getRouting(WhereClause whereClause) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        ImmutableMap.Builder<String, Map<String, Set<Integer>>> builder = ImmutableMap.builder();

        for (DiscoveryNode node : nodes) {
            builder.put(node.id(), ImmutableMap.<String, Set<Integer>>of());
        }

        return new Routing(builder.build());
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    @Override
    public String[] concreteIndices() {
        return PARTITIONS;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return INFOS.values().iterator();
    }
}
