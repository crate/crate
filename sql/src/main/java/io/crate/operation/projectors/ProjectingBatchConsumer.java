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

package io.crate.operation.projectors;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchConsumer;
import io.crate.data.BatchIterator;
import io.crate.data.Projector;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.UUID;

/**
 * Consumer implementation which applies projections onto the BatchIterator received on accept,
 * before passing it onto the lastConsumer.
 *
 * <p>
 * Projections are applied by wrapping the BatchIterators
 * E.g.:
 * </p>
 *
 * <pre>
 *  incomingBatchIterator -> limitingBI(filteringBI(incomingBatchIterator)))
 *
 *  finalConsumer receives limitingBI
 * </pre>
 */
public class ProjectingBatchConsumer implements BatchConsumer {

    private final BatchConsumer consumer;
    private final Collection<? extends Projection> projections;
    private final UUID jobId;
    private final RamAccountingContext ramAccountingContext;
    private final ProjectorFactory projectorFactory;

    public static BatchConsumer create(BatchConsumer lastConsumer,
                                       Collection<? extends Projection> projections,
                                       UUID jobId,
                                       RamAccountingContext ramAccountingContext,
                                       ProjectorFactory projectorFactory) {
        if (projections.isEmpty()) {
            return lastConsumer;
        }
        return new ProjectingBatchConsumer(lastConsumer, projections, jobId, ramAccountingContext, projectorFactory);
    }

    private ProjectingBatchConsumer(BatchConsumer consumer,
                                    Collection<? extends Projection> projections,
                                    UUID jobId,
                                    RamAccountingContext ramAccountingContext,
                                    ProjectorFactory projectorFactory) {
        this.consumer = consumer;
        this.projections = projections;
        this.jobId = jobId;
        this.ramAccountingContext = ramAccountingContext;
        this.projectorFactory = projectorFactory;
    }

    @Override
    public void accept(BatchIterator iterator, @Nullable Throwable failure) {
        if (failure == null) {
            for (Projection projection : projections) {
                Projector projector = projectorFactory.create(projection, ramAccountingContext, jobId);
                iterator = projector.apply(iterator);
            }
            consumer.accept(iterator, null);
        } else {
            consumer.accept(iterator, failure);
        }
    }

    @Override
    public boolean requiresScroll() {
        return consumer.requiresScroll();
    }
}
