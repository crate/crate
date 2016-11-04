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
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.*;
import org.elasticsearch.cluster.service.ClusterService;

import javax.annotation.Nullable;

public class SysNodesTableInfo extends StaticTableInfo {

    public static final String SYS_COL_NAME = "_node";
    public static final ColumnIdent SYS_COL_IDENT = new ColumnIdent(SYS_COL_NAME);

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "nodes");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(new ColumnIdent("id"));

    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

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
    public static final String SYS_COL_OS = "os";
    public static final String SYS_COL_OS_INFO = "os_info";
    public static final String SYS_COL_PROCESS = "process";
    public static final String SYS_COL_FS = "fs";

    private static final DataType OBJECT_ARRAY_TYPE = new ArrayType(DataTypes.OBJECT);

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent(SYS_COL_ID);
        public static final ColumnIdent NAME = new ColumnIdent(SYS_COL_NODE_NAME);
        public static final ColumnIdent HOSTNAME = new ColumnIdent(SYS_COL_HOSTNAME);
        public static final ColumnIdent REST_URL = new ColumnIdent(SYS_COL_REST_URL);

        public static final ColumnIdent PORT = new ColumnIdent(SYS_COL_PORT);
        public static final ColumnIdent PORT_HTTP = new ColumnIdent(SYS_COL_PORT, ImmutableList.of("http"));
        public static final ColumnIdent PORT_TRANSPORT = new ColumnIdent(SYS_COL_PORT, ImmutableList.of("transport"));
        public static final ColumnIdent PORT_PSQL = new ColumnIdent(SYS_COL_PORT, ImmutableList.of("psql"));

        public static final ColumnIdent LOAD = new ColumnIdent(SYS_COL_LOAD);
        public static final ColumnIdent LOAD_1 = new ColumnIdent(SYS_COL_LOAD, ImmutableList.of("1"));
        public static final ColumnIdent LOAD_5 = new ColumnIdent(SYS_COL_LOAD, ImmutableList.of("5"));
        public static final ColumnIdent LOAD_15 = new ColumnIdent(SYS_COL_LOAD, ImmutableList.of("15"));
        public static final ColumnIdent LOAD_PROBE_TS = new ColumnIdent(SYS_COL_LOAD, ImmutableList.of("probe_timestamp"));

        public static final ColumnIdent MEM = new ColumnIdent(SYS_COL_MEM);
        public static final ColumnIdent MEM_FREE = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("free"));
        public static final ColumnIdent MEM_USED = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("used"));
        public static final ColumnIdent MEM_FREE_PERCENT = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("free_percent"));
        public static final ColumnIdent MEM_USED_PERCENT = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("used_percent"));
        public static final ColumnIdent MEM_PROBE_TS = new ColumnIdent(SYS_COL_MEM, ImmutableList.of("probe_timestamp"));

        public static final ColumnIdent HEAP = new ColumnIdent(SYS_COL_HEAP);
        public static final ColumnIdent HEAP_FREE = new ColumnIdent(SYS_COL_HEAP, ImmutableList.of("free"));
        public static final ColumnIdent HEAP_USED = new ColumnIdent(SYS_COL_HEAP, ImmutableList.of("used"));
        public static final ColumnIdent HEAP_MAX = new ColumnIdent(SYS_COL_HEAP, ImmutableList.of("max"));
        public static final ColumnIdent HEAP_PROBE_TS = new ColumnIdent(SYS_COL_HEAP, ImmutableList.of("probe_timestamp"));

        public static final ColumnIdent VERSION = new ColumnIdent(SYS_COL_VERSION);
        public static final ColumnIdent VERSION_NUMBER = new ColumnIdent(SYS_COL_VERSION, ImmutableList.of("number"));
        public static final ColumnIdent VERSION_BUILD_HASH = new ColumnIdent(SYS_COL_VERSION, ImmutableList.of("build_hash"));
        public static final ColumnIdent VERSION_BUILD_SNAPSHOT = new ColumnIdent(SYS_COL_VERSION, ImmutableList.of("build_snapshot"));

        public static final ColumnIdent THREAD_POOLS = new ColumnIdent(SYS_COL_THREAD_POOLS);
        public static final ColumnIdent THREAD_POOLS_NAME = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("name"));
        public static final ColumnIdent THREAD_POOLS_ACTIVE = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("active"));
        public static final ColumnIdent THREAD_POOLS_REJECTED = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("rejected"));
        public static final ColumnIdent THREAD_POOLS_LARGEST = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("largest"));
        public static final ColumnIdent THREAD_POOLS_COMPLETED = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("completed"));
        public static final ColumnIdent THREAD_POOLS_THREADS = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("threads"));
        public static final ColumnIdent THREAD_POOLS_QUEUE = new ColumnIdent(SYS_COL_THREAD_POOLS, ImmutableList.of("queue"));

        public static final ColumnIdent NETWORK = new ColumnIdent(SYS_COL_NETWORK);
        public static final ColumnIdent NETWORK_PROBE_TS = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("probe_timestamp"));
        public static final ColumnIdent NETWORK_TCP = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp"));
        public static final ColumnIdent NETWORK_TCP_CONNECTIONS = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections"));
        public static final ColumnIdent NETWORK_TCP_CONNECTIONS_INITIATED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "initiated"));
        public static final ColumnIdent NETWORK_TCP_CONNECTIONS_ACCEPTED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "accepted"));
        public static final ColumnIdent NETWORK_TCP_CONNECTIONS_CURR_ESTABLISHED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "curr_established"));
        public static final ColumnIdent NETWORK_TCP_CONNECTIONS_DROPPED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "dropped"));
        public static final ColumnIdent NETWORK_TCP_CONNECTIONS_EMBRYONIC_DROPPED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "connections", "embryonic_dropped"));
        public static final ColumnIdent NETWORK_TCP_PACKETS = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets"));
        public static final ColumnIdent NETWORK_TCP_PACKETS_SENT = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "sent"));
        public static final ColumnIdent NETWORK_TCP_PACKETS_RECEIVED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "received"));
        public static final ColumnIdent NETWORK_TCP_PACKETS_RETRANSMITTED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "retransmitted"));
        public static final ColumnIdent NETWORK_TCP_PACKETS_ERRORS_RECEIVED = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "errors_received"));
        public static final ColumnIdent NETWORK_TCP_PACKETS_RST_SENT = new ColumnIdent(SYS_COL_NETWORK, ImmutableList.of("tcp", "packets", "rst_sent"));

        public static final ColumnIdent OS = new ColumnIdent(SYS_COL_OS);
        public static final ColumnIdent OS_UPTIME = new ColumnIdent(SYS_COL_OS, ImmutableList.of("uptime"));
        public static final ColumnIdent OS_TIMESTAMP = new ColumnIdent(SYS_COL_OS, ImmutableList.of("timestamp"));
        public static final ColumnIdent OS_PROBE_TS = new ColumnIdent(SYS_COL_OS, ImmutableList.of("probe_timestamp"));
        public static final ColumnIdent OS_CPU = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu"));
        public static final ColumnIdent OS_CPU_SYSTEM = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "system"));
        public static final ColumnIdent OS_CPU_USER = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "user"));
        public static final ColumnIdent OS_CPU_IDLE = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "idle"));
        public static final ColumnIdent OS_CPU_USED = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "used"));
        public static final ColumnIdent OS_CPU_STOLEN = new ColumnIdent(SYS_COL_OS, ImmutableList.of("cpu", "stolen"));

        public static final ColumnIdent OS_INFO = new ColumnIdent(SYS_COL_OS_INFO);
        public static final ColumnIdent OS_INFO_AVAIL_PROCESSORS = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("available_processors"));
        public static final ColumnIdent OS_INFO_NAME = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("name"));
        public static final ColumnIdent OS_INFO_ARCH = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("arch"));
        public static final ColumnIdent OS_INFO_VERSION = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("version"));
        public static final ColumnIdent OS_INFO_JVM = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm"));
        public static final ColumnIdent OS_INFO_JVM_VERSION = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm", "version"));
        public static final ColumnIdent OS_INFO_JVM_NAME = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm", "vm_name"));
        public static final ColumnIdent OS_INFO_JVM_VENDOR = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm", "vm_vendor"));
        public static final ColumnIdent OS_INFO_JVM_VM_VERSION = new ColumnIdent(SYS_COL_OS_INFO, ImmutableList.of("jvm", "vm_version"));

        public static final ColumnIdent PROCESS = new ColumnIdent(SYS_COL_PROCESS);
        public static final ColumnIdent PROCESS_OPEN_FILE_DESCR = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("open_file_descriptors"));
        public static final ColumnIdent PROCESS_MAX_OPEN_FILE_DESCR = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("max_open_file_descriptors"));
        public static final ColumnIdent PROCESS_PROBE_TS = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("probe_timestamp"));
        public static final ColumnIdent PROCESS_CPU = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("cpu"));
        public static final ColumnIdent PROCESS_CPU_PERCENT = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("cpu", "percent"));
        public static final ColumnIdent PROCESS_CPU_USER = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("cpu", "user"));
        public static final ColumnIdent PROCESS_CPU_SYSTEM = new ColumnIdent(SYS_COL_PROCESS, ImmutableList.of("cpu", "system"));

        public static final ColumnIdent FS = new ColumnIdent(SYS_COL_FS);
        public static final ColumnIdent FS_TOTAL = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total"));
        public static final ColumnIdent FS_TOTAL_SIZE = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "size"));
        public static final ColumnIdent FS_TOTAL_USED = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "used"));
        public static final ColumnIdent FS_TOTAL_AVAILABLE = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "available"));
        public static final ColumnIdent FS_TOTAL_READS = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "reads"));
        public static final ColumnIdent FS_TOTAL_BYTES_READ = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "bytes_read"));
        public static final ColumnIdent FS_TOTAL_WRITES = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "writes"));
        public static final ColumnIdent FS_TOTAL_BYTES_WRITTEN = new ColumnIdent(SYS_COL_FS, ImmutableList.of("total", "bytes_written"));
        public static final ColumnIdent FS_DISKS = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks"));
        public static final ColumnIdent FS_DISKS_DEV = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "dev"));
        public static final ColumnIdent FS_DISKS_SIZE = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "size"));
        public static final ColumnIdent FS_DISKS_USED = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "used"));
        public static final ColumnIdent FS_DISKS_AVAILABLE = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "available"));
        public static final ColumnIdent FS_DISKS_READS = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "reads"));
        public static final ColumnIdent FS_DISKS_BYTES_READ = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "bytes_read"));
        public static final ColumnIdent FS_DISKS_WRITES = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "writes"));
        public static final ColumnIdent FS_DISKS_BYTES_WRITTEN = new ColumnIdent(SYS_COL_FS, ImmutableList.of("disks", "bytes_written"));
        public static final ColumnIdent FS_DATA = new ColumnIdent(SYS_COL_FS, ImmutableList.of("data"));
        public static final ColumnIdent FS_DATA_DEV = new ColumnIdent(SYS_COL_FS, ImmutableList.of("data", "dev"));
        public static final ColumnIdent FS_DATA_PATH = new ColumnIdent(SYS_COL_FS, ImmutableList.of("data", "path"));
    }

    private final TableColumn tableColumn;
    private final ClusterService clusterService;

    public SysNodesTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.ID, DataTypes.STRING)
                .register(Columns.NAME, DataTypes.STRING)
                .register(Columns.HOSTNAME, DataTypes.STRING)
                .register(Columns.REST_URL, DataTypes.STRING)

                .register(Columns.PORT, DataTypes.OBJECT)
                .register(Columns.PORT_HTTP, DataTypes.INTEGER)
                .register(Columns.PORT_TRANSPORT, DataTypes.INTEGER)
                .register(Columns.PORT_PSQL, DataTypes.INTEGER)

                .register(Columns.LOAD, DataTypes.OBJECT)
                .register(Columns.LOAD_1, DataTypes.DOUBLE)
                .register(Columns.LOAD_5, DataTypes.DOUBLE)
                .register(Columns.LOAD_15, DataTypes.DOUBLE)
                .register(Columns.LOAD_PROBE_TS, DataTypes.TIMESTAMP)

                .register(Columns.MEM, DataTypes.OBJECT)
                .register(Columns.MEM_FREE, DataTypes.LONG)
                .register(Columns.MEM_USED, DataTypes.LONG)
                .register(Columns.MEM_FREE_PERCENT, DataTypes.SHORT)
                .register(Columns.MEM_USED_PERCENT, DataTypes.SHORT)
                .register(Columns.MEM_PROBE_TS, DataTypes.TIMESTAMP)

                .register(Columns.HEAP, DataTypes.OBJECT)
                .register(Columns.HEAP_FREE, DataTypes.LONG)
                .register(Columns.HEAP_USED, DataTypes.LONG)
                .register(Columns.HEAP_MAX, DataTypes.LONG)
                .register(Columns.HEAP_PROBE_TS, DataTypes.TIMESTAMP)

                .register(Columns.VERSION, DataTypes.OBJECT)
                .register(Columns.VERSION_NUMBER, StringType.INSTANCE)
                .register(Columns.VERSION_BUILD_HASH, StringType.INSTANCE)
                .register(Columns.VERSION_BUILD_SNAPSHOT, DataTypes.BOOLEAN)

                .register(Columns.THREAD_POOLS, OBJECT_ARRAY_TYPE)
                .register(Columns.THREAD_POOLS_NAME, DataTypes.STRING)
                .register(Columns.THREAD_POOLS_ACTIVE, DataTypes.INTEGER)
                .register(Columns.THREAD_POOLS_REJECTED, DataTypes.LONG)
                .register(Columns.THREAD_POOLS_LARGEST, DataTypes.INTEGER)
                .register(Columns.THREAD_POOLS_COMPLETED, DataTypes.LONG)
                .register(Columns.THREAD_POOLS_THREADS, DataTypes.INTEGER)
                .register(Columns.THREAD_POOLS_QUEUE, DataTypes.INTEGER)

                .register(Columns.NETWORK, DataTypes.OBJECT)
                .register(Columns.NETWORK_PROBE_TS, DataTypes.TIMESTAMP)
                .register(Columns.NETWORK_TCP, DataTypes.OBJECT)
                .register(Columns.NETWORK_TCP_CONNECTIONS, DataTypes.OBJECT)
                .register(Columns.NETWORK_TCP_CONNECTIONS_INITIATED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_CONNECTIONS_ACCEPTED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_CONNECTIONS_CURR_ESTABLISHED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_CONNECTIONS_DROPPED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_CONNECTIONS_EMBRYONIC_DROPPED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS, DataTypes.OBJECT)
                .register(Columns.NETWORK_TCP_PACKETS_SENT, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS_RECEIVED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS_RETRANSMITTED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS_ERRORS_RECEIVED, DataTypes.LONG)
                .register(Columns.NETWORK_TCP_PACKETS_RST_SENT, DataTypes.LONG)

                .register(Columns.OS, DataTypes.OBJECT)
                .register(Columns.OS_UPTIME, DataTypes.LONG)
                .register(Columns.OS_TIMESTAMP, DataTypes.TIMESTAMP)
                .register(Columns.OS_PROBE_TS, DataTypes.TIMESTAMP)
                .register(Columns.OS_CPU, DataTypes.OBJECT)
                .register(Columns.OS_CPU_SYSTEM, DataTypes.SHORT)
                .register(Columns.OS_CPU_USER, DataTypes.SHORT)
                .register(Columns.OS_CPU_IDLE, DataTypes.SHORT)
                .register(Columns.OS_CPU_USED, DataTypes.SHORT)
                .register(Columns.OS_CPU_STOLEN, DataTypes.SHORT)

                .register(Columns.OS_INFO, DataTypes.OBJECT)
                .register(Columns.OS_INFO_AVAIL_PROCESSORS, DataTypes.INTEGER)
                .register(Columns.OS_INFO_NAME, DataTypes.STRING)
                .register(Columns.OS_INFO_ARCH, DataTypes.STRING)
                .register(Columns.OS_INFO_VERSION, DataTypes.STRING)
                .register(Columns.OS_INFO_JVM, DataTypes.OBJECT)
                .register(Columns.OS_INFO_JVM_VERSION, DataTypes.STRING)
                .register(Columns.OS_INFO_JVM_NAME, DataTypes.STRING)
                .register(Columns.OS_INFO_JVM_VENDOR, DataTypes.STRING)
                .register(Columns.OS_INFO_JVM_VM_VERSION, DataTypes.STRING)

                .register(Columns.PROCESS, DataTypes.OBJECT)
                .register(Columns.PROCESS_OPEN_FILE_DESCR, DataTypes.LONG)
                .register(Columns.PROCESS_MAX_OPEN_FILE_DESCR, DataTypes.LONG)
                .register(Columns.PROCESS_PROBE_TS, DataTypes.TIMESTAMP)
                .register(Columns.PROCESS_CPU, DataTypes.OBJECT)
                .register(Columns.PROCESS_CPU_PERCENT, DataTypes.SHORT)
                .register(Columns.PROCESS_CPU_USER, DataTypes.LONG)
                .register(Columns.PROCESS_CPU_SYSTEM, DataTypes.LONG)

                .register(Columns.FS, DataTypes.OBJECT)
                .register(Columns.FS_TOTAL, DataTypes.OBJECT)
                .register(Columns.FS_TOTAL_SIZE, DataTypes.LONG)
                .register(Columns.FS_TOTAL_USED, DataTypes.LONG)
                .register(Columns.FS_TOTAL_AVAILABLE, DataTypes.LONG)
                .register(Columns.FS_TOTAL_READS, DataTypes.LONG)
                .register(Columns.FS_TOTAL_BYTES_READ, DataTypes.LONG)
                .register(Columns.FS_TOTAL_WRITES, DataTypes.LONG)
                .register(Columns.FS_TOTAL_BYTES_WRITTEN, DataTypes.LONG)
                .register(Columns.FS_DISKS, OBJECT_ARRAY_TYPE)
                .register(Columns.FS_DISKS_DEV, DataTypes.STRING)
                .register(Columns.FS_DISKS_SIZE, DataTypes.LONG)
                .register(Columns.FS_DISKS_USED, DataTypes.LONG)
                .register(Columns.FS_DISKS_AVAILABLE, DataTypes.LONG)
                .register(Columns.FS_DISKS_READS, DataTypes.LONG)
                .register(Columns.FS_DISKS_BYTES_READ, DataTypes.LONG)
                .register(Columns.FS_DISKS_WRITES, DataTypes.LONG)
                .register(Columns.FS_DISKS_BYTES_WRITTEN, DataTypes.LONG)
                .register(Columns.FS_DATA, OBJECT_ARRAY_TYPE)
                .register(Columns.FS_DATA_DEV, DataTypes.STRING)
                .register(Columns.FS_DATA_PATH, DataTypes.STRING),
            PRIMARY_KEY);
        this.clusterService = clusterService;
        this.tableColumn = new TableColumn(SYS_COL_IDENT, columnMap);
    }

    public static Reference tableColumnInfo(TableIdent tableIdent) {
        return new Reference(
            new ReferenceIdent(tableIdent, SYS_COL_IDENT),
            RowGranularity.NODE,
            ObjectType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true
        );
    }

    public TableColumn tableColumn() {
        return tableColumn;
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return Routing.forTableOnSingleNode(IDENT, clusterService.localNode().getId());
    }
}
