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
import io.crate.role.metadata.SysPrivilegesTableInfo;
import io.crate.role.metadata.SysRolesTableInfo;
import io.crate.role.metadata.SysUsersTableInfo;

@Singleton
public class SysSchemaInfo implements SchemaInfo {

    public static final String NAME = "sys";
    private final Map<String, TableInfo> tableInfos;

    @Inject
    public SysSchemaInfo(ClusterService clusterService, Roles roles) {
        Supplier<DiscoveryNode> localNode = clusterService::localNode;
        tableInfos = Map.ofEntries(
            Map.entry(SysClusterTableInfo.IDENT.name(), SysClusterTableInfo.of(clusterService)),
            Map.entry(SysNodesTableInfo.IDENT.name(), SysNodesTableInfo.INSTANCE),
            Map.entry(SysShardsTableInfo.IDENT.name(), SysShardsTableInfo.create(roles)),
            Map.entry(SysJobsTableInfo.IDENT.name(), SysJobsTableInfo.create(localNode)),
            Map.entry(SysJobsLogTableInfo.IDENT.name(), SysJobsLogTableInfo.create(localNode)),
            Map.entry(SysOperationsTableInfo.IDENT.name(), SysOperationsTableInfo.create(localNode)),
            Map.entry(SysOperationsLogTableInfo.IDENT.name(), SysOperationsLogTableInfo.INSTANCE),
            Map.entry(SysChecksTableInfo.IDENT.name(), SysChecksTableInfo.INSTANCE),
            Map.entry(SysNodeChecksTableInfo.IDENT.name(), SysNodeChecksTableInfo.INSTANCE),
            Map.entry(SysRepositoriesTableInfo.IDENT.name(), SysRepositoriesTableInfo.create(clusterService.getClusterSettings().maskedSettings())),
            Map.entry(SysSnapshotsTableInfo.IDENT.name(), SysSnapshotsTableInfo.INSTANCE),
            Map.entry(SysSnapshotRestoreTableInfo.IDENT.name(), SysSnapshotRestoreTableInfo.INSTANCE),
            Map.entry(SysSummitsTableInfo.IDENT.name(), SysSummitsTableInfo.INSTANCE),
            Map.entry(SysAllocationsTableInfo.IDENT.name(), SysAllocationsTableInfo.INSTANCE),
            Map.entry(SysHealth.IDENT.name(), SysHealth.INSTANCE),
            Map.entry(SysMetricsTableInfo.NAME.name(), SysMetricsTableInfo.create(localNode)),
            Map.entry(SysSegmentsTableInfo.IDENT.name(), SysSegmentsTableInfo.create(clusterService::localNode)),
            Map.entry(
                SysUsersTableInfo.IDENT.name(),
                SysUsersTableInfo.create(() -> clusterService.state().metadata().clusterUUID())),
            Map.entry(SysRolesTableInfo.IDENT.name(), SysRolesTableInfo.INSTANCE),
            Map.entry(SysPrivilegesTableInfo.IDENT.name(), SysPrivilegesTableInfo.INSTANCE),
            Map.entry(SysSessionsTableInfo.IDENT.name(), SysSessionsTableInfo.create(localNode))
        );
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
}
