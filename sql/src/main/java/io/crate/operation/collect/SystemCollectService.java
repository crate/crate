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
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.sys.SysJobsLogTableInfo;
import io.crate.metadata.sys.SysJobsTableInfo;
import io.crate.metadata.sys.SysOperationsLogTableInfo;
import io.crate.metadata.sys.SysOperationsTableInfo;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.reference.sys.job.RowContextDocLevelReferenceResolver;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Literal;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.DiscoveryService;

import java.util.List;
import java.util.Set;

/**
 * this collect service can be used to retrieve a collector for system tables (which don't contain shards)
 */
public class SystemCollectService implements CollectService {

    private final CollectInputSymbolVisitor<RowContextCollectorExpression<?, ?>> docInputSymbolVisitor;
    private final ImmutableMap<String, StatsTables.IterableGetter> iterableGetters;
    private final DiscoveryService discoveryService;


    @Inject
    public SystemCollectService(DiscoveryService discoveryService, Functions functions, StatsTables statsTables) {
        docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions,
                RowContextDocLevelReferenceResolver.INSTANCE);

        iterableGetters = ImmutableMap.<String, StatsTables.IterableGetter>of(
                SysJobsTableInfo.IDENT.fqn(), statsTables.jobsGetter(),
                SysJobsLogTableInfo.IDENT.fqn(), statsTables.jobsLogGetter(),
                SysOperationsTableInfo.IDENT.fqn(), statsTables.operationsGetter(),
                SysOperationsLogTableInfo.IDENT.fqn(), statsTables.operationsLogGetter()
        );
        this.discoveryService = discoveryService;
    }

    @Override
    public CrateCollector getCollector(CollectNode collectNode, RowDownstream downstream) {
        if (collectNode.whereClause().noMatch()) {
            return new NoopCrateCollector(downstream);
        }
        Set<String> tables = collectNode.routing().locations().get(discoveryService.localNode().id()).keySet();
        assert tables.size() == 1;
        StatsTables.IterableGetter iterableGetter = iterableGetters.get(tables.iterator().next());
        assert iterableGetter != null;
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.process(collectNode);

        Input<Boolean> condition;
        if (collectNode.whereClause().hasQuery()) {
            // TODO: single arg method
            condition = (Input<Boolean>) docInputSymbolVisitor.process(collectNode.whereClause().query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }

        return new SystemTableCollector(
                ctx.topLevelInputs(), ctx.docLevelExpressions(), downstream, iterableGetter.getIterable(), condition);
    }

    static class SystemTableCollector<R> implements CrateCollector {

        private final InputRow row;
        private final List<RowContextCollectorExpression<R, ?>> collectorExpressions;
        private final RowDownstreamHandle downstream;
        private final Iterable<R> rows;
        private final Input<Boolean> condition;

        protected SystemTableCollector(List<Input<?>> inputs,
                                       List<RowContextCollectorExpression<R, ?>> collectorExpressions,
                                       RowDownstream downstream,
                                       Iterable<R> rows,
                                       Input<Boolean> condition) {
            this.row = new InputRow(inputs);
            this.collectorExpressions = collectorExpressions;
            this.rows = rows;
            this.condition = condition;
            assert downstream != null;
            this.downstream = downstream.registerUpstream(this);
        }

        @Override
        public void doCollect(JobCollectContext jobCollectContext) {
            for (R row : rows) {
                for (RowContextCollectorExpression<R, ?> collectorExpression : collectorExpressions) {
                    collectorExpression.setNextRow(row);
                }
                Boolean match = condition.value();
                if (match == null || !match) {
                    // no match
                    continue;
                }

                if (!downstream.setNextRow(this.row)) {
                    // no more rows required, we can stop here
                    break;
                }
            }
            downstream.finish();
            jobCollectContext.close();
        }
    }
}
