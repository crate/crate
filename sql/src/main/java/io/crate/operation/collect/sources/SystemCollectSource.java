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
import io.crate.analyze.symbol.Literal;
import io.crate.metadata.Functions;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.information.*;
import io.crate.metadata.sys.*;
import io.crate.operation.Input;
import io.crate.operation.collect.*;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.RowContextReferenceResolver;
import io.crate.operation.reference.sys.check.SysChecker;
import io.crate.operation.reference.sys.repositories.SysRepositories;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.DiscoveryService;

import java.util.Collection;
import java.util.Set;

/**
 * this collect service can be used to retrieve a collector for system tables (which don't contain shards)
 */
public class SystemCollectSource implements CollectSource {

    private final CollectInputSymbolVisitor<RowCollectExpression<?, ?>> docInputSymbolVisitor;
    private final ImmutableMap<String, IterableGetter> iterableGetters;
    private final DiscoveryService discoveryService;


    @Inject
    public SystemCollectSource(DiscoveryService discoveryService,
                               Functions functions,
                               StatsTables statsTables,
                               InformationSchemaIterables informationSchemaIterables,
                               SysChecker sysChecker,
                               SysRepositories sysRepositories) {
        docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, RowContextReferenceResolver.INSTANCE);

        iterableGetters = ImmutableMap.<String, IterableGetter>builder()
                .put(InformationSchemataTableInfo.IDENT.fqn(), informationSchemaIterables.schemas())
                .put(InformationTablesTableInfo.IDENT.fqn(), informationSchemaIterables.tablesGetter())
                .put(InformationPartitionsTableInfo.IDENT.fqn(), informationSchemaIterables.partitionsGetter())
                .put(InformationColumnsTableInfo.IDENT.fqn(), informationSchemaIterables.columnsGetter())
                .put(InformationTableConstraintsTableInfo.IDENT.fqn(), informationSchemaIterables.constraintsGetter())
                .put(InformationRoutinesTableInfo.IDENT.fqn(), informationSchemaIterables.routinesGetter())
                .put(SysJobsTableInfo.IDENT.fqn(), statsTables.jobsGetter())
                .put(SysJobsLogTableInfo.IDENT.fqn(), statsTables.jobsLogGetter())
                .put(SysOperationsTableInfo.IDENT.fqn(), statsTables.operationsGetter())
                .put(SysOperationsLogTableInfo.IDENT.fqn(), statsTables.operationsLogGetter())
                .put(SysChecksTableInfo.IDENT.fqn(), sysChecker)
                .put(SysRepositoriesTableInfo.IDENT.fqn(), sysRepositories)
                .build();
        this.discoveryService = discoveryService;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        Set<String> tables = collectPhase.routing().locations().get(discoveryService.localNode().id()).keySet();
        assert tables.size() == 1;
        IterableGetter iterableGetter = iterableGetters.get(tables.iterator().next());
        assert iterableGetter != null;
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.extractImplementations(collectPhase);

        Input<Boolean> condition;
        if (collectPhase.whereClause().noMatch()){
            return ImmutableList.<CrateCollector>of(RowsCollector.empty(downstream));
        }
        if (collectPhase.whereClause().hasQuery()) {
            // TODO: single arg method
            condition = (Input<Boolean>) docInputSymbolVisitor.process(collectPhase.whereClause().query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }

        return ImmutableList.<CrateCollector>of(new RowsCollector<>(
                ctx.topLevelInputs(), ctx.docLevelExpressions(), downstream, iterableGetter.getIterable(), condition));
    }
}
