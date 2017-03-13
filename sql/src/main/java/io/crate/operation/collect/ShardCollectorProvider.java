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

package io.crate.operation.collect;

import io.crate.action.job.SharedShardContext;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.AbstractReferenceResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.shard.RecoveryShardReferenceResolver;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.projectors.*;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.Projections;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public abstract class ShardCollectorProvider {

    private final ProjectorFactory projectorFactory;
    private final InputFactory inputFactory;
    final EvaluatingNormalizer shardNormalizer;

    ShardCollectorProvider(ClusterService clusterService,
                           AbstractReferenceResolver shardResolver,
                           Functions functions,
                           IndexNameExpressionResolver indexNameExpressionResolver,
                           ThreadPool threadPool,
                           Settings settings,
                           TransportActionProvider transportActionProvider,
                           BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                           IndexShard indexShard) {
        this.inputFactory = new InputFactory(functions);
        this.shardNormalizer = new EvaluatingNormalizer(
            functions,
            RowGranularity.SHARD,
            ReplaceMode.COPY,
            new RecoveryShardReferenceResolver(shardResolver, indexShard),
            null
        );
        projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            functions,
            indexNameExpressionResolver,
            threadPool,
            settings,
            transportActionProvider,
            bulkRetryCoordinatorPool,
            inputFactory,
            shardNormalizer,
            null,
            indexShard.shardId()
        );
    }

    @Nullable
    public Object[] getRowForShard(RoutedCollectPhase collectPhase) {
        assert collectPhase.maxRowGranularity() == RowGranularity.SHARD : "granularity must be SHARD";

        collectPhase = collectPhase.normalize(shardNormalizer, null);
        if (collectPhase.whereClause().noMatch()) {
            return null;
        }
        assert !collectPhase.whereClause().hasQuery()
            : "whereClause shouldn't have a query after normalize. Should be NO_MATCH or MATCH_ALL";

        InputFactory.Context<CollectExpression<Row, ?>> ctx =
            inputFactory.ctxForInputColumns(collectPhase.toCollect());

        List<Input<?>> inputs = ctx.topLevelInputs();
        Object[] row = new Object[inputs.size()];
        for (int i = 0; i < row.length; i++) {
            row[i] = inputs.get(i).value();
        }
        return row;
    }

    /**
     * Create a CrateCollector.Builder to collect rows from a shard.
     * <p>
     * This also creates all shard-level projectors.
     * The RowReceiver that is used for {@link CrateCollector.Builder#build(RowReceiver)}
     * should be the first node-level projector.
     */
    public CrateCollector.Builder getCollectorBuilder(RoutedCollectPhase collectPhase,
                                                      Set<Requirement> downstreamRequirements,
                                                      JobCollectContext jobCollectContext) throws Exception {
        assert collectPhase.orderBy() ==
               null : "getDocCollector shouldn't be called if there is an orderBy on the collectPhase";
        RoutedCollectPhase normalizedCollectNode = collectPhase.normalize(shardNormalizer, null);

        final CrateCollector.Builder builder;
        if (normalizedCollectNode.whereClause().noMatch()) {
            builder = RowsCollector.builder(Collections.emptyList());
        } else {
            assert normalizedCollectNode.maxRowGranularity() == RowGranularity.DOC : "granularity must be DOC";
            builder = getBuilder(normalizedCollectNode, downstreamRequirements, jobCollectContext);
        }

        Collection<? extends Projection> shardProjections = Projections.shardProjections(collectPhase.projections());
        if (shardProjections.isEmpty()) {
            return builder;
        } else {
            return rowReceiver -> builder.build(ProjectorChain.prependProjectors(
                rowReceiver,
                shardProjections,
                normalizedCollectNode.jobId(),
                jobCollectContext.queryPhaseRamAccountingContext(),
                projectorFactory
            ));
        }
    }

    protected abstract CrateCollector.Builder getBuilder(RoutedCollectPhase collectPhase,
                                                         Set<Requirement> downstreamRequirements,
                                                         JobCollectContext jobCollectContext);


    public abstract OrderedDocCollector getOrderedCollector(RoutedCollectPhase collectPhase,
                                                            SharedShardContext sharedShardContext,
                                                            JobCollectContext jobCollectContext,
                                                            boolean requiresRepeat);
}
