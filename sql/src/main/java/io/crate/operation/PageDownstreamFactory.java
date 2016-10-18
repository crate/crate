/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation;

import com.google.common.base.Optional;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Row;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.operation.merge.IteratorPageDownstream;
import io.crate.operation.merge.PagingIterator;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.operation.merge.SortedPagingIterator;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.node.dql.MergePhase;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Executor;

@Singleton
public class PageDownstreamFactory {

    private final ProjectionToProjectorVisitor projectionToProjectorVisitor;

    @Inject
    public PageDownstreamFactory(ClusterService clusterService,
                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                 ThreadPool threadPool,
                                 Settings settings,
                                 TransportActionProvider transportActionProvider,
                                 BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                 Functions functions) {
        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(functions);
        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY);
        this.projectionToProjectorVisitor = new ProjectionToProjectorVisitor(
            clusterService,
            functions,
            indexNameExpressionResolver,
            threadPool,
            settings,
            transportActionProvider,
            bulkRetryCoordinatorPool,
            implementationSymbolVisitor,
            normalizer
        );
    }

    public ProjectorFactory projectorFactory() {
        return projectionToProjectorVisitor;
    }

    public PageDownstream createMergeNodePageDownstream(MergePhase mergePhase,
                                                        RowReceiver downstream,
                                                        boolean requiresRepeatSupport,
                                                        RamAccountingContext ramAccountingContext,
                                                        Optional<Executor> executorOptional) {
        if (!mergePhase.projections().isEmpty()) {
            FlatProjectorChain projectorChain = FlatProjectorChain.withAttachedDownstream(
                projectionToProjectorVisitor,
                ramAccountingContext,
                mergePhase.projections(),
                downstream,
                mergePhase.jobId()
            );
            downstream = projectorChain.firstProjector();
        }

        PagingIterator<Void, Row> pagingIterator;
        PositionalOrderBy positionalOrderBy = mergePhase.orderByPositions();
        if (positionalOrderBy != null && mergePhase.numUpstreams() > 1) {
            pagingIterator = new SortedPagingIterator<>(
                OrderingByPosition.rowOrdering(positionalOrderBy),
                requiresRepeatSupport
            );
        } else {
            pagingIterator = requiresRepeatSupport ?
                PassThroughPagingIterator.<Void, Row>repeatable() : PassThroughPagingIterator.<Void, Row>oneShot();
        }
        return new IteratorPageDownstream(downstream, pagingIterator, executorOptional);
    }
}
