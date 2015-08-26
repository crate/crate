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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.Functions;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.sys.SysJobsLogTableInfo;
import io.crate.metadata.sys.SysJobsTableInfo;
import io.crate.metadata.sys.SysOperationsLogTableInfo;
import io.crate.metadata.sys.SysOperationsTableInfo;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.reference.sys.job.RowContextDocLevelReferenceResolver;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Literal;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.DiscoveryService;

import java.util.Set;

/**
 * this collect service can be used to retrieve a collector for system tables (which don't contain shards)
 */
public class SystemCollectService implements CollectService {

    private final CollectInputSymbolVisitor<RowCollectExpression<?, ?>> docInputSymbolVisitor;
    private final ImmutableMap<String, StatsTables.IterableGetter> iterableGetters;
    private final DiscoveryService discoveryService;


    @Inject
    public SystemCollectService(DiscoveryService discoveryService, Functions functions, StatsTables statsTables) {
        docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, RowContextDocLevelReferenceResolver.INSTANCE);

        iterableGetters = ImmutableMap.of(
                SysJobsTableInfo.IDENT.fqn(), statsTables.jobsGetter(),
                SysJobsLogTableInfo.IDENT.fqn(), statsTables.jobsLogGetter(),
                SysOperationsTableInfo.IDENT.fqn(), statsTables.operationsGetter(),
                SysOperationsLogTableInfo.IDENT.fqn(), statsTables.operationsLogGetter()
        );
        this.discoveryService = discoveryService;
    }

    @Override
    public CrateCollector getCollector(CollectPhase collectNode, RowDownstream downstream, JobCollectContext jobCollectContext) {
        if (collectNode.whereClause().noMatch()) {
            return new NoopCrateCollector(downstream);
        }
        Set<String> tables = collectNode.routing().locations().get(discoveryService.localNode().id()).keySet();
        assert tables.size() == 1;
        StatsTables.IterableGetter iterableGetter = iterableGetters.get(tables.iterator().next());
        assert iterableGetter != null;
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.extractImplementations(collectNode);

        Input<Boolean> condition;
        if (collectNode.whereClause().hasQuery()) {
            // TODO: single arg method
            condition = (Input<Boolean>) docInputSymbolVisitor.process(collectNode.whereClause().query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }

        return new RowsCollector<>(
                ctx.topLevelInputs(), ctx.docLevelExpressions(), downstream, iterableGetter.getIterable(), condition);
    }
}
