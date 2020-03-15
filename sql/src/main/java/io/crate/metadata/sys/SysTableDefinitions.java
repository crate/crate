/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.sys;

import io.crate.analyze.user.Privilege;
import io.crate.execution.engine.collect.files.SummitsIterable;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.expression.reference.sys.check.SysChecker;
import io.crate.expression.reference.sys.check.node.SysNodeChecks;
import io.crate.expression.reference.sys.shard.ShardSegments;
import io.crate.expression.reference.sys.shard.SysAllocations;
import io.crate.expression.reference.sys.snapshot.SysSnapshots;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.repositories.RepositoriesService;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class SysTableDefinitions {

    private final Map<RelationName, StaticTableDefinition<?>> tableDefinitions = new HashMap<>();

    @Inject
    public SysTableDefinitions(JobsLogs jobsLogs,
                               ClusterService clusterService,
                               SysSchemaInfo sysSchemaInfo,
                               Set<SysCheck> sysChecks,
                               SysNodeChecks sysNodeChecks,
                               RepositoriesService repositoriesService,
                               SysSnapshots sysSnapshots,
                               SysAllocations sysAllocations,
                               ShardSegments shardSegmentInfos,
                               TableHealthService tableHealthService) {
        Supplier<DiscoveryNode> localNode = clusterService::localNode;
        SysClusterTableInfo sysClusterTableInfo = (SysClusterTableInfo) sysSchemaInfo.getTableInfo(SysClusterTableInfo.IDENT.name());
        assert sysClusterTableInfo != null : "sys.cluster table must exist in sys schema";
        tableDefinitions.put(SysClusterTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(Collections.singletonList(null)),
            sysClusterTableInfo.expressions(),
            false
        ));
        var sysJobsTable = SysJobsTableInfo.create(localNode);
        tableDefinitions.put(SysJobsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.activeJobs()),
            sysJobsTable.expressions(),
            (user, jobCtx) -> user.isSuperUser() || user.name().equals(jobCtx.username()),
            false));
        tableDefinitions.put(SysJobsLogTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.jobsLog()),
            SysJobsLogTableInfo.expressions(localNode),
            (user, jobCtx) -> user.isSuperUser() || user.name().equals(jobCtx.username()),
            false));
        tableDefinitions.put(SysOperationsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.activeOperations()),
            SysOperationsTableInfo.expressions(localNode),
            false));
        tableDefinitions.put(SysOperationsLogTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.operationsLog()),
            SysOperationsLogTableInfo.expressions(),
            false));

        SysChecker<SysCheck> sysChecker = new SysChecker<>(sysChecks);
        tableDefinitions.put(SysChecksTableInfo.IDENT, new StaticTableDefinition<>(
            sysChecker::computeResultAndGet,
            SysChecksTableInfo.expressions(),
            true));

        tableDefinitions.put(SysNodeChecksTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(sysNodeChecks),
            SysNodeChecksTableInfo.expressions(),
            true));
        tableDefinitions.put(SysRepositoriesTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(repositoriesService.getRepositoriesList()),
            SysRepositoriesTableInfo.expressions(clusterService.getClusterSettings().maskedSettings()),
            false));
        tableDefinitions.put(SysSnapshotsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(sysSnapshots.currentSnapshots()),
            SysSnapshotsTableInfo.expressions(),
            true));

        tableDefinitions.put(SysAllocationsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> sysAllocations,
            (user, allocation) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, allocation.fqn()),
            SysAllocationsTableInfo.expressions()
        ));

        SummitsIterable summits = new SummitsIterable();
        tableDefinitions.put(SysSummitsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(summits),
            SysSummitsTableInfo.expressions(),
            false));

        SystemTable<TableHealth> sysHealth = SysHealth.create();
        tableDefinitions.put(SysHealth.IDENT, new StaticTableDefinition<>(
            tableHealthService::computeResults,
            sysHealth.expressions(),
            (user, tableHealth) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, tableHealth.fqn()),
            true)
        );
        tableDefinitions.put(SysMetricsTableInfo.NAME, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.metrics()),
            SysMetricsTableInfo.expressions(localNode),
            false));
        tableDefinitions.put(SysSegmentsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(shardSegmentInfos),
            SysSegmentsTableInfo.expressions(clusterService::localNode),
            true));
    }

    public StaticTableDefinition<?> get(RelationName relationName) {
        return tableDefinitions.get(relationName);
    }

    public <R> void registerTableDefinition(RelationName relationName, StaticTableDefinition<R> definition) {
        StaticTableDefinition<?> existingDefinition = tableDefinitions.putIfAbsent(relationName, definition);
        assert existingDefinition == null : "A static table definition is already registered for ident=" + relationName.toString();
    }
}
