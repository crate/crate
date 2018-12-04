/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.collect.sources;

import io.crate.analyze.QueryClause;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class SingleRowSource implements CollectSource {

    private final Functions functions;
    private final EvaluatingNormalizer clusterNormalizer;

    @Inject
    public SingleRowSource(Functions functions, ClusterReferenceResolver clusterRefResolver) {
        this.functions = functions;
        clusterNormalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, clusterRefResolver, null);
    }

    @Override
    public BatchIterator<Row> getIterator(TransactionContext txnCtx,
                                          CollectPhase phase,
                                          CollectTask collectTask,
                                          boolean supportMoveToStart) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        collectPhase = collectPhase.normalize(clusterNormalizer, null);

        if (!QueryClause.canMatch(collectPhase.where())) {
            return InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        }
        assert collectPhase.where().symbolType().isValueSymbol()
            : "whereClause must have been normalized to a value, but is: " + collectPhase.where();

        InputFactory inputFactory = new InputFactory(functions);
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(txnCtx, collectPhase.toCollect());

        return InMemoryBatchIterator.of(new InputRow(ctx.topLevelInputs()), SentinelRow.SENTINEL);
    }
}
