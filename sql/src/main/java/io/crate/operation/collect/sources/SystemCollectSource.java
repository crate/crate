/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.RowsBatchIterator;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.information.*;
import io.crate.metadata.pg_catalog.PgCatalogTables;
import io.crate.metadata.pg_catalog.PgTypeTable;
import io.crate.metadata.sys.*;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.BatchIteratorCollectorBridge;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.collect.files.SummitsIterable;
import io.crate.operation.collect.stats.JobsLogs;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import io.crate.operation.reference.sys.SysRowUpdater;
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.operation.reference.sys.check.SysChecker;
import io.crate.operation.reference.sys.check.node.SysNodeChecks;
import io.crate.operation.reference.sys.node.local.NodeSysExpression;
import io.crate.operation.reference.sys.node.local.NodeSysReferenceResolver;
import io.crate.operation.reference.sys.repositories.SysRepositoriesService;
import io.crate.operation.reference.sys.snapshot.SysSnapshots;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * this collect service can be used to retrieve a collector for system tables (which don't contain shards)
 * <p>
 * System tables are generally represented as Iterable of some type and are converted on-the-fly to {@link Row}
 */
public class SystemCollectSource implements CollectSource {

    private final Functions functions;
    private final NodeSysExpression nodeSysExpression;
    private final ImmutableMap<String, Supplier<CompletableFuture<? extends Iterable<?>>>> iterableGetters;
    private final ImmutableMap<TableIdent, SysRowUpdater<?>> rowUpdaters;
    private final ClusterService clusterService;
    private final InputFactory inputFactory;

    @Inject
    public SystemCollectSource(ClusterService clusterService,
                               Functions functions,
                               NodeSysExpression nodeSysExpression,
                               JobsLogs jobsLogs,
                               InformationSchemaIterables informationSchemaIterables,
                               Set<SysCheck> sysChecks,
                               SysNodeChecks sysNodeChecks,
                               SysRepositoriesService sysRepositoriesService,
                               SysSnapshots sysSnapshots,
                               PgCatalogTables pgCatalogTables) {
        this.clusterService = clusterService;
        inputFactory = new InputFactory(functions);
        this.functions = functions;
        this.nodeSysExpression = nodeSysExpression;

        rowUpdaters = ImmutableMap.of(SysNodeChecksTableInfo.IDENT, sysNodeChecks);

        iterableGetters = ImmutableMap.<String, Supplier<CompletableFuture<? extends Iterable<?>>>>builder()
            .put(InformationSchemataTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(informationSchemaIterables.schemas()))
            .put(InformationTablesTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(informationSchemaIterables.tables()))
            .put(InformationPartitionsTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(informationSchemaIterables.partitions()))
            .put(InformationColumnsTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(informationSchemaIterables.columns()))
            .put(InformationTableConstraintsTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(informationSchemaIterables.constraints()))
            .put(InformationRoutinesTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(informationSchemaIterables.routines()))
            .put(InformationSqlFeaturesTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(informationSchemaIterables.features()))
            .put(SysJobsTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(jobsLogs.jobsGetter()))
            .put(SysJobsLogTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(jobsLogs.jobsLogGetter()))
            .put(SysOperationsTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(jobsLogs.operationsGetter()))
            .put(SysOperationsLogTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(jobsLogs.operationsLogGetter()))
            .put(SysChecksTableInfo.IDENT.fqn(),
                () -> new SysChecker<>(sysChecks).checksGetter())
            .put(SysNodeChecksTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(sysNodeChecks))
            .put(SysRepositoriesTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(sysRepositoriesService.repositoriesGetter()))
            .put(SysSnapshotsTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(sysSnapshots.snapshotsGetter()))
            .put(SysSummitsTableInfo.IDENT.fqn(),
                () -> synchronousIterableGetter(new SummitsIterable().summitsGetter()))
            .put(PgTypeTable.IDENT.fqn(),
                () -> synchronousIterableGetter(pgCatalogTables.typesGetter()))
            .build();
    }

    private CompletableFuture<? extends Iterable<?>> synchronousIterableGetter(Iterable<?> iterable) {
        return CompletableFuture.completedFuture(iterable);
    }

    Function<Iterable, Iterable<? extends Row>> toRowsIterableTransformation(RoutedCollectPhase collectPhase,
                                                                             boolean requiresRepeat) {
        return objects -> dataIterableToRowsIterable(collectPhase, requiresRepeat, objects);
    }

    private Iterable<? extends Row> dataIterableToRowsIterable(RoutedCollectPhase collectPhase,
                                                               boolean requiresRepeat,
                                                               Iterable<?> data) {
        if (requiresRepeat) {
            data = ImmutableList.copyOf(data);
        }
        return RowsTransformer.toRowsIterable(
            inputFactory,
            RowContextReferenceResolver.INSTANCE,
            collectPhase,
            data);
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase phase,
                                                    BatchConsumer consumer,
                                                    JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        // sys.operations can contain a _node column - these refs need to be normalized into literals
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions, RowGranularity.DOC, ReplaceMode.COPY, new NodeSysReferenceResolver(nodeSysExpression), null);
        final RoutedCollectPhase routedCollectPhase = collectPhase.normalize(normalizer, null);

        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        String table = Iterables.getOnlyElement(locations.get(clusterService.localNode().getId()).keySet());
        Supplier<CompletableFuture<? extends Iterable<?>>> iterableGetter = iterableGetters.get(table);
        assert iterableGetter != null : "iterableGetter for " + table + " must exist";
        boolean requiresScroll = consumer.requiresScroll();
        return ImmutableList.of(
            BatchIteratorCollectorBridge.newInstance(
                () -> iterableGetter.get().thenApply(dataIterable ->
                    RowsBatchIterator.newInstance(
                        dataIterableToRowsIterable(routedCollectPhase, requiresScroll, dataIterable),
                        collectPhase.toCollect().size()
                    )),
                consumer
            )
        );
    }

    /**
     * Returns a new updater for a given table.
     *
     * @param ident the ident of the table
     * @return a row updater instance for the given table
     */
    public SysRowUpdater<?> getRowUpdater(TableIdent ident) {
        assert rowUpdaters.containsKey(ident) : "RowUpdater for " + ident.fqn() + " must exist";
        return rowUpdaters.get(ident);
    }
}
