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

import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.CrateCollector;
import io.crate.execution.engine.collect.JobCollectContext;
import io.crate.execution.engine.collect.RowsCollector;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
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
    public CrateCollector getCollector(CollectPhase phase, RowConsumer consumer, JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        collectPhase = collectPhase.normalize(clusterNormalizer, null);
        if (collectPhase.whereClause().noMatch()) {
            return RowsCollector.empty(consumer);
        }
        assert !collectPhase.whereClause().hasQuery()
            : "WhereClause should have been normalized to either MATCH_ALL or NO_MATCH";

        InputFactory inputFactory = new InputFactory(functions);
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(collectPhase.toCollect());
        return RowsCollector.single(new InputRow(ctx.topLevelInputs()), consumer);
    }
}
