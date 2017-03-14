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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.metadata.ClusterReferenceResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.RowGranularity;
import io.crate.operation.InputFactory;
import io.crate.operation.InputRow;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsCollector;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Collection;
import java.util.Collections;

@Singleton
public class SingleRowSource implements CollectSource {

    private final Functions functions;
    private final EvaluatingNormalizer clusterNormalizer;

    @Inject
    public SingleRowSource(Functions functions, ClusterReferenceResolver clusterRefResolver) {
        this.functions = functions;
        clusterNormalizer = new EvaluatingNormalizer(
            functions, RowGranularity.CLUSTER, ReplaceMode.COPY, clusterRefResolver, null);
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase phase, BatchConsumer consumer, JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        collectPhase = collectPhase.normalize(clusterNormalizer, null);
        if (collectPhase.whereClause().noMatch()) {
            return ImmutableList.<CrateCollector>of(RowsCollector.empty(consumer));
        }
        assert !collectPhase.whereClause().hasQuery()
            : "WhereClause should have been normalized to either MATCH_ALL or NO_MATCH";

        InputFactory inputFactory = new InputFactory(functions);
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns(collectPhase.toCollect());
        return Collections.singletonList(RowsCollector.single(new InputRow(ctx.topLevelInputs()), consumer));
    }
}
