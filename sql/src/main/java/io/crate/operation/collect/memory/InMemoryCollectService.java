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

package io.crate.operation.collect.memory;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.Functions;
import io.crate.metadata.InMemoryCollectorExpression;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CollectService;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.sys.job.InMemoryDocLevelReferenceResolver;
import io.crate.operation.reference.sys.job.JobContext;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Literal;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.common.inject.Inject;

import java.util.List;

public class InMemoryCollectService implements CollectService {

    private final CollectInputSymbolVisitor<InMemoryCollectorExpression<?, ?>> docInputSymbolVisitor;
    private final ImmutableMap<String, Iterable<?>> iterables;
    private final HashMapTableProvider tableProvider;

    private final Iterable<JobContext> jobsIterable;

    @Inject
    public InMemoryCollectService(Functions functions, HashMapTableProvider tableProvider) {
        docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions,
                InMemoryDocLevelReferenceResolver.INSTANCE);
        this.tableProvider = tableProvider;

        jobsIterable = this.tableProvider.<JobContext>get("jobs", true).values();

        iterables = ImmutableMap.<String, Iterable<?>>of(
                "jobs", jobsIterable
        );

    }

    @Override
    public CrateCollector getCollector(CollectNode collectNode, Projector downstream) {
        assert collectNode.routing().tableIdent() != null;
        if (collectNode.whereClause().noMatch()) {
            return CrateCollector.NOOP;
        }
        Iterable<?> iterator = iterables.get(collectNode.routing().tableIdent().name());
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.process(collectNode);

        Input<Boolean> condition;
        if (collectNode.whereClause().hasQuery()) {
            // TODO: single arg method
            condition = (Input<Boolean>) docInputSymbolVisitor.process(collectNode.whereClause().query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }

        return new InMemoryCollector(
                ctx.topLevelInputs(), ctx.docLevelExpressions(), downstream, iterator, condition);
    }

    static class InMemoryCollector<R> implements CrateCollector {

        private final List<Input<?>> inputs;
        private final List<InMemoryCollectorExpression<R, ?>> collectorExpressions;
        private Projector downstream;
        private final Iterable<R> rows;
        private final Input<Boolean> condition;

        protected InMemoryCollector(List<Input<?>> inputs,
                                             List<InMemoryCollectorExpression<R, ?>> collectorExpressions,
                                             Projector downstream,
                                             Iterable<R> rows,
                                             Input<Boolean> condition) {
            this.inputs = inputs;
            this.collectorExpressions = collectorExpressions;
            this.rows = rows;
            this.condition = condition;
            assert downstream != null;
            downstream(downstream);
        }

        @Override
        public void doCollect() throws Exception {
            for (R row : rows) {
                for (InMemoryCollectorExpression<R, ?> collectorExpression : collectorExpressions) {
                    collectorExpression.setNextRow(row);
                }
                Boolean match = condition.value();
                if (match == null || !match) {
                    // no match
                    continue;
                }

                Object[] newRow = new Object[inputs.size()];
                int i = 0;
                for (Input<?> input : inputs) {
                    newRow[i++] = input.value();
                }
                if (!downstream.setNextRow(newRow)) {
                    // no more rows required, we can stop here
                    downstream.upstreamFinished();
                    throw new CollectionTerminatedException();
                }
            }
            downstream.upstreamFinished();
        }

        @Override
        public void downstream(Projector downstream) {
            downstream.registerUpstream(this);
            this.downstream = downstream;
        }

        @Override
        public Projector downstream() {
            return downstream;
        }
    }
}
