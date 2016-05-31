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
import com.google.common.collect.*;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.metadata.Functions;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.information.*;
import io.crate.metadata.sys.*;
import io.crate.operation.Input;
import io.crate.operation.collect.*;
import io.crate.operation.projectors.InputCondition;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import io.crate.operation.reference.sys.check.SysCheck;
import io.crate.operation.reference.sys.check.SysChecker;
import io.crate.operation.reference.sys.check.SysNodeCheck;
import io.crate.operation.reference.sys.repositories.SysRepositories;
import io.crate.operation.reference.sys.snapshot.SysSnapshots;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.DiscoveryService;

import java.util.*;

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
                               SysSnapshots sysSnapshots) {
        docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, RowContextReferenceResolver.INSTANCE);

        iterableGetters = ImmutableMap.<String, Supplier<Iterable<?>>>builder()
            .put(InformationSchemataTableInfo.IDENT.fqn(), informationSchemaIterables.schemas())
            .put(InformationTablesTableInfo.IDENT.fqn(), informationSchemaIterables.tables())
            .put(InformationPartitionsTableInfo.IDENT.fqn(), informationSchemaIterables.partitions())
            .put(InformationColumnsTableInfo.IDENT.fqn(), informationSchemaIterables.columns())
            .put(InformationTableConstraintsTableInfo.IDENT.fqn(), informationSchemaIterables.constraints())
            .put(InformationRoutinesTableInfo.IDENT.fqn(), informationSchemaIterables.routines())
            .put(SysJobsTableInfo.IDENT.fqn(), statsTables.jobsGetter())
            .put(SysJobsLogTableInfo.IDENT.fqn(), statsTables.jobsLogGetter())
            .put(SysOperationsTableInfo.IDENT.fqn(), statsTables.operationsGetter())
            .put(SysOperationsLogTableInfo.IDENT.fqn(), statsTables.operationsLogGetter())
            .put(SysChecksTableInfo.IDENT.fqn(), new SysChecker(sysChecks))
            .put(SysNodeChecksTableInfo.IDENT.fqn(), new SysChecker(sysNodeChecks))
            .put(SysRepositoriesTableInfo.IDENT.fqn(), sysRepositories)
            .put(SysSnapshotsTableInfo.IDENT.fqn(), sysSnapshots)
            .build();
        this.discoveryService = discoveryService;
    }

    public Iterable<Row> toRowsIterable(RoutedCollectPhase collectPhase, Iterable<?> iterable) {
        WhereClause whereClause = collectPhase.whereClause();
        if (whereClause.noMatch()) {
            return Collections.emptyList();
        }

        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.extractImplementations(collectPhase.toCollect());
        OrderBy orderBy = collectPhase.orderBy();
        if (orderBy != null) {
            for (Symbol symbol : orderBy.orderBySymbols()) {
                docInputSymbolVisitor.process(symbol, ctx);
            }
        }

        Input<Boolean> condition;
        if (whereClause.hasQuery()) {
            assert DataTypes.BOOLEAN.equals(whereClause.query().valueType());
            //noinspection unchecked  whereClause().query() is a symbol of type boolean so it must become Input<Boolean>
            condition = (Input<Boolean>) docInputSymbolVisitor.process(whereClause.query(), ctx);
        } else {
            condition = Literal.BOOLEAN_TRUE;
        }

        @SuppressWarnings("unchecked")
        Iterable<Row> rows = Iterables.filter(
            Iterables.transform(iterable, new ValueAndInputRow<>(ctx.topLevelInputs(), ctx.docLevelExpressions())),
            InputCondition.asPredicate(condition));

        if (orderBy == null) {
            return rows;
        }
        return sortRows(Iterables.transform(rows, Row.MATERIALIZE), collectPhase);
    }

    static Iterable<Row> sortRows(Iterable<Object[]> rows, RoutedCollectPhase collectPhase) {
        ArrayList<Object[]> objects = Lists.newArrayList(rows);
        Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(collectPhase);
        Collections.sort(objects, ordering.reverse());
        return Iterables.transform(objects, Buckets.arrayToRowFunction());
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase phase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        String table = Iterables.getOnlyElement(locations.get(discoveryService.localNode().id()).keySet());
        Supplier<Iterable<?>> iterableGetter = iterableGetters.get(table);
        assert iterableGetter != null : "iterableGetter for " + table + " must exist";
        return ImmutableList.<CrateCollector>of(
            new RowsCollector(downstream, toRowsIterable(collectPhase, iterableGetter.get())));
    }
}
