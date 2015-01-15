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

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Iterator;

@Singleton
public class SysSchemaInfo implements SchemaInfo {

    public static final String NAME = "sys";
    private final ImmutableMap<String, SysTableInfo> tableInfos;

    @Inject
    public SysSchemaInfo(ClusterService clusterService) {

        SysNodesTableInfo sysNodesTableInfo = new SysNodesTableInfo(clusterService, this);


        tableInfos = ImmutableMap.<String, SysTableInfo>builder()
            .put(SysClusterTableInfo.IDENT.name(), new SysClusterTableInfo(clusterService, this))
            .put(SysNodesTableInfo.IDENT.name(), sysNodesTableInfo)
            .put(SysShardsTableInfo.IDENT.name(), new SysShardsTableInfo(clusterService, this, sysNodesTableInfo))
            .put(SysJobsTableInfo.IDENT.name(), new SysJobsTableInfo(clusterService, this))
            .put(SysJobsLogTableInfo.IDENT.name(), new SysJobsLogTableInfo(clusterService, this))
            .put(SysOperationsTableInfo.IDENT.name(), new SysOperationsTableInfo(clusterService, this))
            .put(SysOperationsLogTableInfo.IDENT.name(), new SysOperationsLogTableInfo(clusterService, this))
        .build();
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
    public boolean systemSchema() {
        return true;
    }

    @Override
    public Iterator<? extends TableInfo> iterator() {
        return tableInfos.values().iterator();
    }
}