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
import io.crate.data.BatchIterator;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

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
    private final List<Projector> projectors;
    private boolean requiresScroll;

    public static RowConsumer create(RowConsumer lastConsumer,
                                     Collection<? extends Projection> projections,
                                     UUID jobId,
                                     RamAccountingContext ramAccountingContext,
                                     ProjectorFactory projectorFactory) {
        if (projections.isEmpty()) {
            return lastConsumer;
        }
        return new ProjectingRowConsumer(lastConsumer, projections, jobId, ramAccountingContext, projectorFactory);
    }

    private ProjectingRowConsumer(RowConsumer consumer,
                                  Collection<? extends Projection> projections,
                                  UUID jobId,
                                  RamAccountingContext ramAccountingContext,
                                  ProjectorFactory projectorFactory) {
        this.consumer = consumer;
        projectors = new ArrayList<>(projections.size());

        boolean projectorsSupportIndependentScrolling = false;
        for (Projection projection : projections) {
            Projector projector = projectorFactory.create(projection, ramAccountingContext, jobId);
            projectors.add(projector);

            if (projector.providesIndependentScroll()) {
                projectorsSupportIndependentScrolling = true;
            }
        }
        if (consumer.requiresScroll() && !projectorsSupportIndependentScrolling) {
            requiresScroll = true;
        } else {
            requiresScroll = false;
        }
    }

    @Override
    public void accept(BatchIterator<Row> iterator, @Nullable Throwable failure) {
        if (failure == null) {
            for (Projector projector : projectors) {
                try {
                    iterator = projector.apply(iterator);
                } catch (Throwable t) {
                    consumer.accept(null, t);
                    return;
                }
            }
            consumer.accept(iterator, null);
        } else {
            consumer.accept(iterator, failure);
        }
    }

    @Override
    public boolean requiresScroll() {
        return requiresScroll;
    }
}
