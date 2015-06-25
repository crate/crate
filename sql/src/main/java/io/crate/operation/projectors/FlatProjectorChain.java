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
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.planner.projection.Projection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * A chain of connected projectors rows will flow through.
 *
 * Will be constructed from a list of projections which will be transformed to
 * projectors which will be connected.
 *
 * Usage:
 * <ul>
 * <li> construct it,
 * <li> call {@linkplain #startProjections(ExecutionState)},
 * <li> get the first projector using {@linkplain #firstProjector()}
 * <li> feed data to it,
 * <li> wait for the result of  your custom downstream
 * </ul>
 */
public class FlatProjectorChain {

    private final List<Projector> projectors;

    private FlatProjectorChain(List<Projector> projectors) {
        Preconditions.checkArgument(!projectors.isEmpty(), "no projectors given");
        this.projectors = projectors;
    }

    public void startProjections(ExecutionState executionState) {
        for (Projector projector : Lists.reverse(projectors)) {
            projector.startProjection(executionState);
        }
    }

    public Projector firstProjector() {
        return projectors.get(0);
    }

    /**
     * No ResultProvider will be added.
     * if <code>downstream</code> is a Projector, {@linkplain Projector#startProjection(ExecutionState)} will not be called
     * by this FlatProjectorChain.
     */
    public static FlatProjectorChain withAttachedDownstream(final ProjectorFactory projectorFactory,
                                                            final RamAccountingContext ramAccountingContext,
                                                            Collection<Projection> projections,
                                                            RowDownstream downstream,
                                                            UUID jobId) {
        List<Projector> localProjectors = new ArrayList<>();
        Projector previousProjector = null;
        for (Projection projection : projections) {
            Projector projector = projectorFactory.create(projection, ramAccountingContext, jobId);
            localProjectors.add(projector);
            if (previousProjector != null) {
                previousProjector.downstream(projector);
            }
            previousProjector = projector;
        }
        if (previousProjector != null) {
            previousProjector.downstream(downstream);
        }
        return new FlatProjectorChain(localProjectors);
    }


    /**
     * Create a task from a list of projectors (that is already chained).
     * chains created with this method might not have a ResultProvider, if the last
     * Projector in the list is none.
     */
    public static FlatProjectorChain withProjectors(List<Projector> projectors) {
        return new FlatProjectorChain(projectors);
    }
}
