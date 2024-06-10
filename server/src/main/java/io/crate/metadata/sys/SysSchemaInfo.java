/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.metadata.view.ViewInfo;
import io.crate.role.Roles;

@Singleton
public class SysSchemaInfo implements SchemaInfo {

    public static final String NAME = "sys";
    private final Map<String, TableInfo> tableInfos;

    @Inject
    public SysSchemaInfo(ClusterService clusterService, Roles roles) {
        tableInfos = new HashMap<>();
        tableInfos.put(SysClusterTableInfo.IDENT.name(), SysClusterTableInfo.of(clusterService));
        tableInfos.put(SysNodesTableInfo.IDENT.name(), SysNodesTableInfo.create());
        tableInfos.put(SysShardsTableInfo.IDENT.name(), SysShardsTableInfo.create(roles));
        Supplier<DiscoveryNode> localNode = clusterService::localNode;
        tableInfos.put(SysJobsTableInfo.IDENT.name(), SysJobsTableInfo.create(localNode));
        tableInfos.put(SysJobsLogTableInfo.IDENT.name(), SysJobsLogTableInfo.create(localNode));
        tableInfos.put(SysOperationsTableInfo.IDENT.name(), SysOperationsTableInfo.create(localNode));
        tableInfos.put(SysOperationsLogTableInfo.IDENT.name(), SysOperationsLogTableInfo.create());
        tableInfos.put(SysChecksTableInfo.IDENT.name(), SysChecksTableInfo.create());
        tableInfos.put(SysNodeChecksTableInfo.IDENT.name(), SysNodeChecksTableInfo.create());
        tableInfos.put(SysRepositoriesTableInfo.IDENT.name(), SysRepositoriesTableInfo.create(clusterService.getClusterSettings().maskedSettings()));
        tableInfos.put(SysSnapshotsTableInfo.IDENT.name(), SysSnapshotsTableInfo.create());
        tableInfos.put(SysSnapshotRestoreTableInfo.IDENT.name(), SysSnapshotRestoreTableInfo.create());
        tableInfos.put(SysSummitsTableInfo.IDENT.name(), SysSummitsTableInfo.create());
        tableInfos.put(SysAllocationsTableInfo.IDENT.name(), SysAllocationsTableInfo.create());
        tableInfos.put(SysHealth.IDENT.name(), SysHealth.create());
        tableInfos.put(SysMetricsTableInfo.NAME.name(), SysMetricsTableInfo.create(localNode));
        tableInfos.put(SysSegmentsTableInfo.IDENT.name(), SysSegmentsTableInfo.create(clusterService::localNode));
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
    public Iterable<TableInfo> getTables() {
        return tableInfos.values();
    }

    @Override
    public Iterable<ViewInfo> getViews() {
        return Collections.emptyList();
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
