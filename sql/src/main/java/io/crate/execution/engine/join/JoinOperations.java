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

package io.crate.execution.engine.join;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.PlannerContext;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class JoinOperations {

    private JoinOperations() {
    }

    public static boolean isMergePhaseNeeded(Collection<String> executionNodes,
                                             ResultDescription resultDescription,
                                             boolean isDistributed) {
        return isDistributed ||
               resultDescription.hasRemainingLimitOrOffset() ||
               !resultDescription.nodeIds().equals(executionNodes);
    }

    public static MergePhase buildMergePhaseForJoin(PlannerContext plannerContext,
                                                    ResultDescription resultDescription,
                                                    Collection<String> executionNodes) {
        List<Projection> projections = Collections.emptyList();
        if (resultDescription.hasRemainingLimitOrOffset()) {
            projections = Collections.singletonList(ProjectionBuilder.topNOrEvalIfNeeded(
                resultDescription.limit(),
                resultDescription.offset(),
                resultDescription.numOutputs(),
                resultDescription.streamOutputs()
            ));
        }

        return new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "join-merge",
            resultDescription.nodeIds().size(),
            1,
            executionNodes,
            resultDescription.streamOutputs(),
            projections,
            DistributionInfo.DEFAULT_SAME_NODE,
            resultDescription.orderBy()
        );
    }

    static RowConsumer getBatchConsumer(CompletableFuture<BatchIterator<Row>> future, boolean requiresRepeat) {
        return new RowConsumer() {
            @Override
            public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
                if (failure == null) {
                    future.complete(iterator);
                } else {
                    future.completeExceptionally(failure);
                }
            }

            @Override
            public boolean requiresScroll() {
                return requiresRepeat;
            }
        };
    }

    /**
     * Creates an {@link EvalProjection} to ensure that the join output symbols are emitted in the original order as
     * a possible outer operator (e.g. GROUP BY) is relying on the order.
     * The order could have been changed due to the switch-table optimizations
     *
     * @param outputs       List of join output symbols in their original order.
     * @param joinOutputs   List of join output symbols after possible re-ordering due optimizations.
     */
    public static Projection createJoinProjection(List<Symbol> outputs, List<Symbol> joinOutputs) {
        List<Symbol> projectionOutputs = InputColumns.create(
            outputs,
            new InputColumns.SourceSymbols(joinOutputs));
        return new EvalProjection(projectionOutputs);
    }
}
