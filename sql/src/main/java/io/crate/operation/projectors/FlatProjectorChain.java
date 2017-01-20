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

package io.crate.operation.projectors;

import com.google.common.base.Preconditions;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.data.BatchConsumer;
import io.crate.operation.data.BatchCursor;
import io.crate.operation.data.BatchProjector;
import io.crate.planner.projection.Projection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * A chain of connected projectors rows will flow through.
 * <p>
 * Will be constructed from a list of projections which will be transformed to
 * projectors which will be connected.
 * <p>
 * Usage:
 * <ul>
 * <li> construct it,
 * <li> get the first projector using {@linkplain #firstProjector()}
 * <li> feed data to it,
 * <li> wait for the result of  your custom downstream
 * </ul>
 */
public class FlatProjectorChain {

    public static class Builder {
        private final UUID jobId;
        private final RamAccountingContext ramAccountingContext;
        private final ProjectorFactory projectorFactory;
        private final Collection<? extends Projection> projections;

        public Builder(UUID jobId,
                       RamAccountingContext ramAccountingContext,
                       ProjectorFactory projectorFactory,
                       Collection<? extends Projection> projections) {
            this.jobId = jobId;
            this.ramAccountingContext = ramAccountingContext;
            this.projectorFactory = projectorFactory;
            this.projections = projections;
        }

        public BatchConsumer build(BatchConsumer rowReceiver) {
            return FlatProjectorChain.withAttachedDownstream(
                projectorFactory,
                ramAccountingContext,
                projections,
                rowReceiver,
                jobId
            );
        }
    }

    public static BatchConsumer withAttachedDownstream(final ProjectorFactory projectorFactory,
                                                       final RamAccountingContext ramAccountingContext,
                                                       Collection<? extends Projection> projections,
                                                       BatchConsumer downstream,
                                                       UUID jobId) {
        if (projections.isEmpty()) {
            return downstream;
        }
        List<BatchProjector> projectors = new ArrayList<>(projections.size());
        for (Projection projection : projections) {
            projectors.add(projectorFactory.create(projection, ramAccountingContext, jobId));
        }
        return downstream.projected(projectors);
    }


//    /**
//     * Create a task from a list of rowReceivers (which are already chained).
//     */
//    public static FlatProjectorChain withReceivers(List<? extends RowReceiver> rowReceivers) {
//        return new FlatProjectorChain(rowReceivers);
//    }
}
