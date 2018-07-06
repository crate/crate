/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect;

import io.crate.analyze.QueryClause;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.Projections;
import io.crate.execution.engine.collect.collectors.OrderedDocCollector;
import io.crate.execution.engine.pipeline.ProjectionToProjectorVisitor;
import io.crate.execution.engine.pipeline.ProjectorFactory;
import io.crate.execution.engine.pipeline.Projectors;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.NestableInput;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.List;

public abstract class ShardCollectorProvider {

    private final ProjectorFactory projectorFactory;
    private final InputFactory inputFactory;
    final EvaluatingNormalizer shardNormalizer;

    ShardCollectorProvider(ClusterService clusterService,
                           NodeJobsCounter nodeJobsCounter,
                           ReferenceResolver<NestableInput<?>> shardResolver,
                           Functions functions,
                           ThreadPool threadPool,
                           Settings settings,
                           TransportActionProvider transportActionProvider,
                           IndexShard indexShard,
                           BigArrays bigArrays) {
        this.inputFactory = new InputFactory(functions);
        this.shardNormalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.SHARD,
            shardResolver,
            null
        );
        projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            nodeJobsCounter,
            functions,
            threadPool,
            settings,
            transportActionProvider,
            inputFactory,
            shardNormalizer,
            t -> null,
            t -> null,
            indexShard.indexSettings().getIndexVersionCreated(),
            bigArrays,
            indexShard.shardId()
        );
    }

    @Nullable
    public Object[] getRowForShard(RoutedCollectPhase collectPhase) {
        assert collectPhase.maxRowGranularity() == RowGranularity.SHARD : "granularity must be SHARD";

        collectPhase = collectPhase.normalize(shardNormalizer, null);
        if (!QueryClause.canMatch(collectPhase.where())) {
            return null;
        }
        assert collectPhase.where().symbolType().isValueSymbol() : "where must be a literal after normalize";

        InputFactory.Context<CollectExpression<Row, ?>> ctx =
            inputFactory.ctxForInputColumns(collectPhase.toCollect());

        List<Input<?>> inputs = ctx.topLevelInputs();
        Object[] row = new Object[inputs.size()];
        for (int i = 0; i < row.length; i++) {
            row[i] = inputs.get(i).value();
        }
        return row;
    }

    public BatchIterator<Row> getIterator(RoutedCollectPhase collectPhase,
                                          boolean requiresScroll,
                                          CollectTask collectTask) throws Exception {
        assert collectPhase.orderBy() == null
            : "getDocCollector shouldn't be called if there is an orderBy on the collectPhase";
        assert collectPhase.maxRowGranularity() == RowGranularity.DOC :
            "granularity must be DOC";

        RoutedCollectPhase normalizedCollectNode = collectPhase.normalize(shardNormalizer, null);
        BatchIterator<Row> fusedIterator = getProjectionFusedIterator(normalizedCollectNode, collectTask);
        if (fusedIterator != null) {
            return fusedIterator;
        }
        final BatchIterator<Row> iterator;
        if (QueryClause.canMatch(normalizedCollectNode.where())) {
            iterator = getUnorderedIterator(normalizedCollectNode, requiresScroll, collectTask);
        } else {
            iterator = InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        }
        return Projectors.wrap(
            Projections.shardProjections(collectPhase.projections()),
            collectPhase.jobId(),
            collectTask.queryPhaseRamAccountingContext(),
            projectorFactory,
            iterator
        );
    }

    /**
     * @return A BatchIterator which already applies the transformation described in the shardProjections of the collectPhase.
     *         This can be used to return a specialized BatchIterator for certain projections. If this returns null
     *         a fallback/default BatchIterator will be created and projections will be applied using the projectorFactory
     */
    @Nullable
    protected abstract BatchIterator<Row> getProjectionFusedIterator(RoutedCollectPhase normalizedPhase,
                                                                     CollectTask collectTask);

    protected abstract BatchIterator<Row> getUnorderedIterator(RoutedCollectPhase collectPhase,
                                                               boolean requiresScroll,
                                                               CollectTask collectTask);


    public abstract OrderedDocCollector getOrderedCollector(RoutedCollectPhase collectPhase,
                                                            SharedShardContext sharedShardContext,
                                                            CollectTask collectTask,
                                                            boolean requiresRepeat);

    public ProjectorFactory getProjectorFactory() {
        return projectorFactory;
    }
}
