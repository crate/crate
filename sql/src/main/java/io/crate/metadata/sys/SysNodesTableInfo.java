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
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.*;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import javax.annotation.Nullable;
import java.util.*;

public class SysNodesTableInfo extends SysTableInfo {

    public static final String SYS_COL_NAME = "_node";
    public static final ColumnIdent SYS_COL_IDENT = new ColumnIdent(SYS_COL_NAME);

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "nodes");
    private final TableColumn tableColumn;

    private static final ImmutableList<ColumnIdent> primaryKey = ImmutableList.of(
            new ColumnIdent("id"));

    private final Map<ColumnIdent, ReferenceInfo> infos;
    private final Set<ReferenceInfo> columns;

    public static final String SYS_COL_ID = "id";
    public static final String SYS_COL_NODE_NAME = "name";
    public static final String SYS_COL_HOSTNAME = "hostname";
    public static final String SYS_COL_REST_URL = "rest_url";
    public static final String SYS_COL_PORT = "port";
    public static final String SYS_COL_LOAD = "load";
    public static final String SYS_COL_MEM = "mem";
    public static final String SYS_COL_HEAP = "heap";
    public static final String SYS_COL_VERSION = "version";
    public static final String SYS_COL_THREAD_POOLS = "thread_pools";
    public static final String SYS_COL_NETWORK = "network";
    public static final String SYS_COL_NETWORK_TCP = "tcp";
    public static final String SYS_COL_OS = "os";
    public static final String SYS_COL_OS_CPU = "cpu";
    public static final String SYS_COL_OS_INFO = "os_info";
    public static final String SYS_COL_OS_INFO_JVM = "jvm";
    public static final String SYS_COL_PROCESS = "process";
    public static final String SYS_COL_PROCESS_CPU = "cpu";
    public static final String SYS_COL_FS = "fs";
    public static final String SYS_COL_FS_TOTAL = "total";
    public static final String SYS_COL_FS_DISKS = "disks";
    public static final String SYS_COL_FS_DATA = "data";


    public SysNodesTableInfo(ClusterService service) {
        super(service);
        DataType objectArrayType = new ArrayType(DataTypes.OBJECT);

        ColumnRegistrar registrar = new ColumnRegistrar(IDENT, RowGranularity.NODE)
           .register(SYS_COL_ID, DataTypes.STRING, null)
           .register(SYS_COL_NODE_NAME, DataTypes.STRING, null)
           .register(SYS_COL_HOSTNAME, DataTypes.STRING, null)
           .register(SYS_COL_REST_URL, DataTypes.STRING, null)

           .register(SYS_COL_PORT, DataTypes.OBJECT, null)
           .register(SYS_COL_PORT, DataTypes.INTEGER, ImmutableList.of("http"))
           .register(SYS_COL_PORT, DataTypes.INTEGER, ImmutableList.of("transport"))

           .register(SYS_COL_LOAD, DataTypes.OBJECT, null)
           .register(SYS_COL_LOAD, DataTypes.DOUBLE, ImmutableList.of("1"))
           .register(SYS_COL_LOAD, DataTypes.DOUBLE, ImmutableList.of("5"))
           .register(SYS_COL_LOAD, DataTypes.DOUBLE, ImmutableList.of("15"))
           .register(SYS_COL_LOAD, DataTypes.TIMESTAMP, ImmutableList.of("probe_timestamp"))

           .register(SYS_COL_MEM, DataTypes.OBJECT, null)
           .register(SYS_COL_MEM, DataTypes.LONG, ImmutableList.of("free"))
           .register(SYS_COL_MEM, DataTypes.LONG, ImmutableList.of("used"))
           .register(SYS_COL_MEM, DataTypes.SHORT, ImmutableList.of("free_percent"))
           .register(SYS_COL_MEM, DataTypes.SHORT, ImmutableList.of("used_percent"))
           .register(SYS_COL_MEM, DataTypes.TIMESTAMP, ImmutableList.of("probe_timestamp"))

           .register(SYS_COL_HEAP, DataTypes.OBJECT, null)
           .register(SYS_COL_HEAP, DataTypes.LONG, ImmutableList.of("free"))
           .register(SYS_COL_HEAP, DataTypes.LONG, ImmutableList.of("used"))
           .register(SYS_COL_HEAP, DataTypes.LONG, ImmutableList.of("max"))
           .register(SYS_COL_HEAP, DataTypes.TIMESTAMP, ImmutableList.of("probe_timestamp"))

           .register(SYS_COL_VERSION, DataTypes.OBJECT, null)
           .register(SYS_COL_VERSION, StringType.INSTANCE, ImmutableList.of("number"))
           .register(SYS_COL_VERSION, StringType.INSTANCE, ImmutableList.of("build_hash"))
           .register(SYS_COL_VERSION, DataTypes.BOOLEAN, ImmutableList.of("build_snapshot"))

           .register(SYS_COL_THREAD_POOLS, objectArrayType, null)
           .register(SYS_COL_THREAD_POOLS, StringType.INSTANCE, ImmutableList.of("name"))
           .register(SYS_COL_THREAD_POOLS, IntegerType.INSTANCE, ImmutableList.of("active"))
           .register(SYS_COL_THREAD_POOLS, LongType.INSTANCE, ImmutableList.of("rejected"))
           .register(SYS_COL_THREAD_POOLS, IntegerType.INSTANCE, ImmutableList.of("largest"))
           .register(SYS_COL_THREAD_POOLS, LongType.INSTANCE, ImmutableList.of("completed"))
           .register(SYS_COL_THREAD_POOLS, IntegerType.INSTANCE, ImmutableList.of("threads"))
           .register(SYS_COL_THREAD_POOLS, IntegerType.INSTANCE, ImmutableList.of("queue"))

           .register(SYS_COL_NETWORK, DataTypes.OBJECT, null)
           .register(SYS_COL_NETWORK, DataTypes.TIMESTAMP, ImmutableList.of("probe_timestamp"))
           .register(SYS_COL_NETWORK, DataTypes.OBJECT, ImmutableList.of("tcp"))
           .register(SYS_COL_NETWORK, DataTypes.OBJECT, ImmutableList.of("tcp", "connections"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "connections", "initiated"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "connections", "accepted"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "connections", "curr_established"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "connections", "dropped"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "connections", "embryonic_dropped"))
           .register(SYS_COL_NETWORK, DataTypes.OBJECT, ImmutableList.of("tcp", "packets"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "packets", "sent"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "packets", "received"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "packets", "retransmitted"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "packets", "errors_received"))
           .register(SYS_COL_NETWORK, DataTypes.LONG, ImmutableList.of("tcp", "packets", "rst_sent"))

           .register(SYS_COL_OS, DataTypes.OBJECT, null)
           .register(SYS_COL_OS, DataTypes.LONG, ImmutableList.of("uptime"))
           .register(SYS_COL_OS, DataTypes.TIMESTAMP, ImmutableList.of("timestamp"))
           .register(SYS_COL_OS, DataTypes.TIMESTAMP, ImmutableList.of("probe_timestamp"))
           .register(SYS_COL_OS, DataTypes.OBJECT, ImmutableList.of("cpu"))
           .register(SYS_COL_OS, DataTypes.SHORT, ImmutableList.of("cpu", "system"))
           .register(SYS_COL_OS, DataTypes.SHORT, ImmutableList.of("cpu", "user"))
           .register(SYS_COL_OS, DataTypes.SHORT, ImmutableList.of("cpu", "idle"))
           .register(SYS_COL_OS, DataTypes.SHORT, ImmutableList.of("cpu", "used"))
           .register(SYS_COL_OS, DataTypes.SHORT, ImmutableList.of("cpu", "stolen"))

           .register(SYS_COL_OS_INFO, DataTypes.OBJECT, null)
           .register(SYS_COL_OS_INFO, DataTypes.INTEGER, ImmutableList.of("available_processors"))
           .register(SYS_COL_OS_INFO, DataTypes.STRING, ImmutableList.of("name"))
           .register(SYS_COL_OS_INFO, DataTypes.STRING, ImmutableList.of("arch"))
           .register(SYS_COL_OS_INFO, DataTypes.STRING, ImmutableList.of(SYS_COL_VERSION))
           .register(SYS_COL_OS_INFO, DataTypes.OBJECT, ImmutableList.of("jvm"))
           .register(SYS_COL_OS_INFO, DataTypes.STRING, ImmutableList.of("jvm", SYS_COL_VERSION))
           .register(SYS_COL_OS_INFO, DataTypes.STRING, ImmutableList.of("jvm", "vm_name"))
           .register(SYS_COL_OS_INFO, DataTypes.STRING, ImmutableList.of("jvm", "vm_vendor"))
           .register(SYS_COL_OS_INFO, DataTypes.STRING, ImmutableList.of("jvm", "vm_version"))

           .register(SYS_COL_PROCESS, DataTypes.OBJECT, null)
           .register(SYS_COL_PROCESS, DataTypes.LONG, ImmutableList.of("open_file_descriptors"))
           .register(SYS_COL_PROCESS, DataTypes.LONG, ImmutableList.of("max_open_file_descriptors"))
           .register(SYS_COL_PROCESS, DataTypes.TIMESTAMP, ImmutableList.of("probe_timestamp"))
           .register(SYS_COL_PROCESS, DataTypes.OBJECT, ImmutableList.of("cpu"))
           .register(SYS_COL_PROCESS, DataTypes.SHORT, ImmutableList.of("cpu", "percent"))
           .register(SYS_COL_PROCESS, DataTypes.LONG, ImmutableList.of("cpu", "user"))
           .register(SYS_COL_PROCESS, DataTypes.LONG, ImmutableList.of("cpu", "system"))

           .register(SYS_COL_FS, DataTypes.OBJECT, null)
           .register(SYS_COL_FS, DataTypes.OBJECT, ImmutableList.of("total"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("total", "size"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("total", "used"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("total", "available"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("total", "reads"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("total", "bytes_read"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("total", "writes"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("total", "bytes_written"))

           .register(SYS_COL_FS, objectArrayType, ImmutableList.of("disks"))
           .register(SYS_COL_FS, DataTypes.STRING, ImmutableList.of("disks", "dev"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("disks", "size"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("disks", "used"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("disks", "available"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("disks", "reads"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("disks", "bytes_read"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("disks", "writes"))
           .register(SYS_COL_FS, DataTypes.LONG, ImmutableList.of("disks", "bytes_written"))

           .register(SYS_COL_FS, objectArrayType, ImmutableList.of("data"))
           .register(SYS_COL_FS, DataTypes.STRING, ImmutableList.of("data", "dev"))
           .register(SYS_COL_FS, DataTypes.STRING, ImmutableList.of("data", "path"));

        infos = registrar.infos();
        columns = registrar.columns();
        this.tableColumn = new TableColumn(SYS_COL_IDENT, infos);
    }


    public static ReferenceInfo tableColumnInfo(TableIdent tableIdent){
        return new ReferenceInfo(new ReferenceIdent(tableIdent, SYS_COL_IDENT),
                RowGranularity.NODE, ObjectType.INSTANCE, ColumnPolicy.STRICT,
                ReferenceInfo.IndexType.NOT_ANALYZED);
    }

    public TableColumn tableColumn(){
        return tableColumn;
    }

    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return infos.get(columnIdent);
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
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        TreeMapBuilder<String, Map<String, List<Integer>>> builder = TreeMapBuilder.newMapBuilder();

        for (DiscoveryNode node : nodes) {
            builder.put(node.id(), new TreeMap<String, List<Integer>>());
        }

        return new Routing(builder.map());
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return infos.values().iterator();
    }


}
