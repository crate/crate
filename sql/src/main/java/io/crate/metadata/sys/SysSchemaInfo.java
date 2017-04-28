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

import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Singleton
public class SysSchemaInfo implements SchemaInfo {

    public static final String NAME = "sys";
    private final Map<String, TableInfo> tableInfos;

    @Inject
    public SysSchemaInfo(ClusterService clusterService) {

        SysNodesTableInfo sysNodesTableInfo = new SysNodesTableInfo(clusterService);

        tableInfos = new HashMap<>();
        tableInfos.put(SysClusterTableInfo.IDENT.name(), new SysClusterTableInfo(clusterService));
        tableInfos.put(SysNodesTableInfo.IDENT.name(), sysNodesTableInfo);
        tableInfos.put(SysShardsTableInfo.IDENT.name(), new SysShardsTableInfo(clusterService, sysNodesTableInfo));
        tableInfos.put(SysJobsTableInfo.IDENT.name(), new SysJobsTableInfo(clusterService));
        tableInfos.put(SysJobsLogTableInfo.IDENT.name(), new SysJobsLogTableInfo(clusterService));
        tableInfos.put(SysOperationsTableInfo.IDENT.name(), new SysOperationsTableInfo(clusterService, sysNodesTableInfo));
        tableInfos.put(SysOperationsLogTableInfo.IDENT.name(), new SysOperationsLogTableInfo(clusterService));
        tableInfos.put(SysChecksTableInfo.IDENT.name(), new SysChecksTableInfo(clusterService));
        tableInfos.put(SysNodeChecksTableInfo.IDENT.name(), new SysNodeChecksTableInfo(clusterService));
        tableInfos.put(SysRepositoriesTableInfo.IDENT.name(), new SysRepositoriesTableInfo(clusterService));
        tableInfos.put(SysSnapshotsTableInfo.IDENT.name(), new SysSnapshotsTableInfo(clusterService));
        tableInfos.put(SysSummitsTableInfo.IDENT.name(), new SysSummitsTableInfo(clusterService));
    }

    @Override
    public TableInfo getTableInfo(String name) {
        return tableInfos.get(name);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void invalidateTableCache(String tableName) {

    }

    @Override
    @Nonnull
    public Iterator<TableInfo> iterator() {
        return tableInfos.values().iterator();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void update(ClusterChangedEvent event) {

    }

    public void registerSysTable(TableInfo tableInfo) {
        assert tableInfo.ident().schema().equals("sys") : "table is not in sys schema";
        assert !tableInfos.containsKey(tableInfo.ident().name()) : "table already exists";
        tableInfos.put(tableInfo.ident().name(), tableInfo);
    }
}
