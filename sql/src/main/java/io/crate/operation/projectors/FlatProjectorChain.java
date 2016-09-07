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
import com.google.common.collect.Lists;
import io.crate.breaker.RamAccountingContext;
import io.crate.metadata.RowGranularity;
import io.crate.planner.projection.Projection;

import java.util.*;

/**
 * A chain of connected projectors rows will flow through.
 *
 * Will be constructed from a list of projections which will be transformed to
 * projectors which will be connected.
 *
 * Usage:
 * <ul>
 * <li> construct it,
 * <li> call {@linkplain #prepare()},
 * <li> get the first projector using {@linkplain #firstProjector()}
 * <li> feed data to it,
 * <li> wait for the result of  your custom downstream
 * </ul>
 */
public class FlatProjectorChain {

    private final List<? extends RowReceiver> rowReceivers;

    private FlatProjectorChain(List<? extends RowReceiver> rowReceivers) {
        Preconditions.checkArgument(!rowReceivers.isEmpty(), "no projectors given");
        this.rowReceivers = rowReceivers;
    }

    public void prepare() {
        for (RowReceiver rowReceiver : Lists.reverse(rowReceivers)) {
            rowReceiver.prepare();
        }
    }

    public RowReceiver firstProjector() {
        return rowReceivers.get(0);
    }

    public static FlatProjectorChain withAttachedDownstream(final ProjectorFactory projectorFactory,
                                                            final RamAccountingContext ramAccountingContext,
                                                            Collection<? extends Projection> projections,
                                                            RowReceiver downstream,
                                                            UUID jobId) {
        if (projections.isEmpty()) {
            return new FlatProjectorChain(Collections.singletonList(downstream));
        }
        List<RowReceiver> rowReceivers = new ArrayList<>(projections.size() + 1);
        Projector previousProjector = null;
        for (Projection projection : projections) {
            assert projection.requiredGranularity().ordinal() < RowGranularity.SHARD.ordinal()
                : "FlatProjectorChain doesn't work with projections which require a Shard context";

            Projector projector = projectorFactory.create(projection, ramAccountingContext, jobId);
            rowReceivers.add(projector);
            if (previousProjector != null) {
                previousProjector.downstream(projector);
            }
            previousProjector = projector;
        }
        if (previousProjector != null) {
            rowReceivers.add(downstream);
            previousProjector.downstream(downstream);
        }
        return new FlatProjectorChain(rowReceivers);
    }


    /**
     * Create a task from a list of rowReceivers (which are already chained).
     */
    public static FlatProjectorChain withReceivers(List<? extends RowReceiver> rowReceivers) {
        return new FlatProjectorChain(rowReceivers);
    }
}
