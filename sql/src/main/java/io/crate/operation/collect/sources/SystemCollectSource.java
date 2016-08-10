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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.crate.core.collections.Row;
import io.crate.metadata.Functions;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.information.*;
import io.crate.metadata.pg_catalog.PgCatalogTables;
import io.crate.metadata.pg_catalog.PgTypeTable;
import io.crate.metadata.sys.*;
import io.crate.operation.collect.*;
import io.crate.operation.projectors.Requirement;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.operation.reference.sys.check.SysChecker;
import io.crate.operation.reference.sys.check.SysNodeCheck;
import io.crate.operation.reference.sys.repositories.SysRepositories;
import io.crate.operation.reference.sys.snapshot.SysSnapshots;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.DiscoveryService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * this collect service can be used to retrieve a collector for system tables (which don't contain shards)
 *
 * System tables are generally represented as Iterable of some type and are converted on-the-fly to {@link Row}
 */
public class SystemCollectSource implements CollectSource {

    private final CollectInputSymbolVisitor<RowCollectExpression<?, ?>> docInputSymbolVisitor;
    private final ImmutableMap<String, Supplier<Iterable<?>>> iterableGetters;
    private final DiscoveryService discoveryService;


    @Inject
    public SystemCollectSource(DiscoveryService discoveryService,
                               Functions functions,
                               StatsTables statsTables,
                               InformationSchemaIterables informationSchemaIterables,
                               Set<SysCheck> sysChecks,
                               Set<SysNodeCheck> sysNodeChecks,
                               SysRepositories sysRepositories,
                               SysSnapshots sysSnapshots,
                               PgCatalogTables pgCatalogTables) {
        docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, RowContextReferenceResolver.INSTANCE);

        iterableGetters = ImmutableMap.<String, Supplier<Iterable<?>>>builder()
            .put(InformationSchemataTableInfo.IDENT.fqn(), informationSchemaIterables.schemas())
            .put(InformationTablesTableInfo.IDENT.fqn(), informationSchemaIterables.tables())
            .put(InformationPartitionsTableInfo.IDENT.fqn(), informationSchemaIterables.partitions())
            .put(InformationColumnsTableInfo.IDENT.fqn(), informationSchemaIterables.columns())
            .put(InformationTableConstraintsTableInfo.IDENT.fqn(), informationSchemaIterables.constraints())
            .put(InformationRoutinesTableInfo.IDENT.fqn(), informationSchemaIterables.routines())
            .put(InformationSqlFeaturesTableInfo.IDENT.fqn(), informationSchemaIterables.features())
            .put(SysJobsTableInfo.IDENT.fqn(), statsTables.jobsGetter())
            .put(SysJobsLogTableInfo.IDENT.fqn(), statsTables.jobsLogGetter())
            .put(SysOperationsTableInfo.IDENT.fqn(), statsTables.operationsGetter())
            .put(SysOperationsLogTableInfo.IDENT.fqn(), statsTables.operationsLogGetter())
            .put(SysChecksTableInfo.IDENT.fqn(), new SysChecker(sysChecks))
            .put(SysNodeChecksTableInfo.IDENT.fqn(), new SysChecker(sysNodeChecks))
            .put(SysRepositoriesTableInfo.IDENT.fqn(), sysRepositories)
            .put(SysSnapshotsTableInfo.IDENT.fqn(), sysSnapshots)
            .put(PgTypeTable.IDENT.fqn(), pgCatalogTables.pgTypes())
            .build();
        this.discoveryService = discoveryService;
    }

    Iterable<Row> toRowsIterable(RoutedCollectPhase collectPhase, Iterable<?> iterable, boolean requiresRepeat) {
        if (requiresRepeat) {
            iterable = ImmutableList.copyOf(iterable);
        }
        return RowsTransformer.toRowsIterable(docInputSymbolVisitor, collectPhase, iterable);
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase phase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        String table = Iterables.getOnlyElement(locations.get(discoveryService.localNode().id()).keySet());
        Supplier<Iterable<?>> iterableGetter = iterableGetters.get(table);
        assert iterableGetter != null : "iterableGetter for " + table + " must exist";
        return ImmutableList.<CrateCollector>of(
            new RowsCollector(downstream, toRowsIterable(collectPhase, iterableGetter.get(),
                downstream.requirements().contains(Requirement.REPEAT))));
    }
}
