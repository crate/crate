/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.pipeline;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.jetbrains.annotations.Nullable;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.projection.Projection;
import io.crate.memory.MemoryManager;
import io.crate.metadata.TransactionContext;

/**
 * Consumer implementation which applies projections onto the BatchIterator received on accept,
 * before passing it onto the lastConsumer.
 * <p>
 * <p>
 * Projections are applied by wrapping the BatchIterators
 * E.g.:
 * </p>
 * <p>
 * <pre>
 *  incomingBatchIterator -> limitingBI(filteringBI(incomingBatchIterator)))
 *
 *  finalConsumer receives limitingBI
 * </pre>
 */
public class ProjectingRowConsumer implements RowConsumer {

    private final RowConsumer consumer;
    private final Projectors projectors;

    /**
     * Wraps the {@param lastConsumer} with a ProjectingRowConsumer which applies the applicable projections.
     * Only the projections with requiredGranularity less or equal than the supportedGranularity of the
     * {@param projectorFactory} are applied and the rest are ignored. E.g.:
     *
     * <pre>
     *  If
     *      projection1.requiredGranularity = NODE
     *      projection2.requiredGranularity = SHARD
     *      projectionFactory.supportedGranularity = NODE
     *  Then
     *      projection1 is applied
     *      projection2 is ignored
     * </pre>
     *
     * @param lastConsumer The {@link RowConsumer} so far in the pipeline
     * @param projections The projections that we try to apply
     * @param jobId The jobId of the job being executing
     * @param ramAccounting The {@link RamAccounting} used for memory calculation for the {@link CircuitBreaker}
     * @param projectorFactory The {@link ProjectorFactory} that will create the appropriate projectors from the {@param projections}
     * @return the {@link ProjectingRowConsumer} wrapping the existing {@param lastConsumer}
     */
    public static RowConsumer create(RowConsumer lastConsumer,
                                     Collection<? extends Projection> projections,
                                     UUID jobId,
                                     TransactionContext txnCtx,
                                     RamAccounting ramAccounting,
                                     MemoryManager memoryManager,
                                     ProjectorFactory projectorFactory) {
        if (projections.isEmpty()) {
            return lastConsumer;
        }
        return new ProjectingRowConsumer(lastConsumer, projections, jobId, txnCtx, ramAccounting, memoryManager, projectorFactory);
    }

    private ProjectingRowConsumer(RowConsumer consumer,
                                  Collection<? extends Projection> projections,
                                  UUID jobId,
                                  TransactionContext txnCtx,
                                  RamAccounting ramAccounting,
                                  MemoryManager memoryManager,
                                  ProjectorFactory projectorFactory) {
        this.consumer = consumer;
        this.projectors = new Projectors(projections, jobId, txnCtx, ramAccounting, memoryManager, projectorFactory);
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (failure == null) {
            try {
                iterator = projectors.wrap(iterator);
            } catch (Throwable t) {
                consumer.accept(null, t);
                return;
            }
            consumer.accept(iterator, null);
        } else {
            consumer.accept(iterator, failure);
        }
    }

    @Override
    public CompletableFuture<?> completionFuture() {
        return consumer.completionFuture();
    }

    @Override
    public void kill(Throwable throwable) {
        consumer.kill(throwable);
    }

    @Override
    public boolean requiresScroll() {
        return consumer.requiresScroll() && !projectors.providesIndependentScroll();
    }
}
