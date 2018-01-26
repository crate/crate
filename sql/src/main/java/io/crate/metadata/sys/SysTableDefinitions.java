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

import com.google.common.annotations.VisibleForTesting;
import io.crate.analyze.user.Privilege;
import io.crate.metadata.TableIdent;
import io.crate.execution.engine.collect.files.SummitsIterable;
import io.crate.execution.engine.collect.stats.JobsLogs;
import io.crate.expression.reference.StaticTableDefinition;
import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.expression.reference.sys.check.SysChecker;
import io.crate.expression.reference.sys.check.node.SysNodeChecks;
import io.crate.expression.reference.sys.shard.SysAllocations;
import io.crate.expression.reference.sys.snapshot.SysSnapshot;
import io.crate.expression.reference.sys.snapshot.SysSnapshots;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.repositories.RepositoriesService;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class SysTableDefinitions {

    private final Map<TableIdent, StaticTableDefinition<?>> tableDefinitions = new HashMap<>();

    @Inject
    public SysTableDefinitions(JobsLogs jobsLogs,
                               Set<SysCheck> sysChecks,
                               SysNodeChecks sysNodeChecks,
                               RepositoriesService repositoriesService,
                               SysSnapshots sysSnapshots,
                               SysAllocations sysAllocations) {
        tableDefinitions.put(SysJobsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.activeJobs()),
            SysJobsTableInfo.expressions()
        ));
        tableDefinitions.put(SysJobsLogTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.jobsLog()),
            SysJobsLogTableInfo.expressions()
        ));
        tableDefinitions.put(SysOperationsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.activeOperations()),
            SysOperationsTableInfo.expressions()
        ));
        tableDefinitions.put(SysOperationsLogTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(jobsLogs.operationsLog()),
            SysOperationsLogTableInfo.expressions()
        ));

        SysChecker<SysCheck> sysChecker = new SysChecker<>(sysChecks);
        tableDefinitions.put(SysChecksTableInfo.IDENT, new StaticTableDefinition<>(
            sysChecker::computeResultAndGet,
            SysChecksTableInfo.expressions()
        ));

        tableDefinitions.put(SysNodeChecksTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(sysNodeChecks),
            SysNodeChecksTableInfo.expressions()
        ));
        tableDefinitions.put(SysRepositoriesTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(repositoriesService.getRepositoriesList()),
            SysRepositoriesTableInfo.expressions()
        ));
        tableDefinitions.put(SysSnapshotsTableInfo.IDENT, new StaticTableDefinition<>(
            snapshotSupplier(sysSnapshots),
            SysSnapshotsTableInfo.expressions()
        ));

        tableDefinitions.put(SysAllocationsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> sysAllocations,
            (user, allocation) -> user.hasAnyPrivilege(Privilege.Clazz.TABLE, allocation.fqn()),
            SysAllocationsTableInfo.expressions()
        ));

        SummitsIterable summits = new SummitsIterable();
        tableDefinitions.put(SysSummitsTableInfo.IDENT, new StaticTableDefinition<>(
            () -> completedFuture(summits),
            SysSummitsTableInfo.expressions()
        ));
    }

    public StaticTableDefinition<?> get(TableIdent tableIdent) {
        return tableDefinitions.get(tableIdent);
    }

    public <R> void registerTableDefinition(TableIdent tableIdent, StaticTableDefinition<R> definition) {
        StaticTableDefinition<?> existingDefinition = tableDefinitions.putIfAbsent(tableIdent, definition);
        assert existingDefinition == null : "A static table definition is already registered for ident=" + tableIdent.toString();
    }

    @VisibleForTesting
    public static Supplier<CompletableFuture<? extends Iterable<SysSnapshot>>> snapshotSupplier(SysSnapshots sysSnapshots) {
        return () -> {
            CompletableFuture<Iterable<SysSnapshot>> f = new CompletableFuture<>();
            try {
                f.complete(sysSnapshots.snapshotsGetter());
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
            return f;
        };
    }
}
