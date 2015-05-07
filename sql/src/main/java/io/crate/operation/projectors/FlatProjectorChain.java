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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.RowDownstream;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
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
 * <li> call {@linkplain #startProjections()},
 * <li> get the first projector using {@linkplain #firstProjector()}
 * <li> feed data to it,
 * <li> wait for the result of {@linkplain #resultProvider()} or your custom downstream
 * </ul>
 */
public class FlatProjectorChain {

    private final List<Projector> projectors;
    private final Optional<ResultProvider> resultProvider;

    private FlatProjectorChain(List<Projector> projectors, Optional<ResultProvider> resultProvider) {
        this.projectors = projectors;
        this.resultProvider = resultProvider;
    }

    public void startProjections() {
        for (Projector projector : Lists.reverse(projectors)) {
            projector.startProjection();
        }
    }

    public Projector firstProjector() {
        return projectors.get(0);
    }

    @Nullable
    public ResultProvider resultProvider() {
        return resultProvider.orNull();
    }

    /**
     * No ResultProvider will be added.
     * if <code>downstream</code> is a Projector, {@linkplain io.crate.operation.projectors.Projector#startProjection()} will not be called
     * by this FlatProjectorChain.
     */
    public static FlatProjectorChain withAttachedDownstream(final ProjectionToProjectorVisitor projectorVisitor,
                                                            final RamAccountingContext ramAccountingContext,
                                                            Collection<Projection> projections,
                                                            RowDownstream downstream) {
        return withAttachedDownstream(
                projectorVisitor,
                ramAccountingContext,
                projections,
                downstream,
                Optional.<UUID>absent()
        );
    }

    public static FlatProjectorChain withAttachedDownstream(final ProjectionToProjectorVisitor projectorVisitor,
                                                            final RamAccountingContext ramAccountingContext,
                                                            Collection<Projection> projections,
                                                            RowDownstream downstream,
                                                            Optional<UUID> jobId) {
        Preconditions.checkArgument(!projections.isEmpty(), "no projections given");
        return create(projectorVisitor, ramAccountingContext, projections, downstream, false, jobId);

    }

    /**
     * attach a ResultProvider if the last projector is none
     */
    public static FlatProjectorChain withResultProvider(ProjectionToProjectorVisitor projectorVisitor,
                                                        RamAccountingContext ramAccountingContext,
                                                        Collection<Projection> projections) {
        return withResultProvider(projectorVisitor, ramAccountingContext, projections, Optional.<UUID>absent());
    }

    public static FlatProjectorChain withResultProvider(ProjectionToProjectorVisitor projectorVisitor,
                                                        RamAccountingContext ramAccountingContext,
                                                        Collection<Projection> projections,
                                                        Optional<UUID> jobId) {

        return create(projectorVisitor, ramAccountingContext, projections, null, true, jobId);
    }

    /**
     * Create a task from a list of projectors (that is already chained).
     * chains created with this method might not have a ResultProvider, if the last
     * Projector in the list is none.
     */
    public static FlatProjectorChain withProjectors(List<Projector> projectors) {
        Preconditions.checkArgument(!projectors.isEmpty(), "no projectors given");
        Projector last = projectors.get(projectors.size()-1);
        return new FlatProjectorChain(projectors, resultProviderOptional(last));
    }

    private static FlatProjectorChain create(ProjectionToProjectorVisitor projectorVisitor,
                                             RamAccountingContext ramAccountingContext,
                                             Collection<Projection> projections,
                                             @Nullable RowDownstream rowDownstream,
                                             boolean addResultProviderIfPresent,
                                             Optional<UUID> jobId) {
        assert (rowDownstream == null && addResultProviderIfPresent) || (rowDownstream != null && !addResultProviderIfPresent);
        Preconditions.checkArgument(!projections.isEmpty() || addResultProviderIfPresent, "no projections given");
        List<Projector> localProjectors = new ArrayList<>();
        Projector previousProjector = null;
        for (Projection projection : projections) {
            Projector projector = projectorVisitor.process(
                    projection,
                    ramAccountingContext,
                    jobId
            );
            localProjectors.add(projector);
            if (previousProjector != null) {
                previousProjector.downstream(projector);
            }
            previousProjector = projector;
        }
        if (rowDownstream != null) {
            if (previousProjector != null) {
                previousProjector.downstream(rowDownstream);
            }
        } else if (addResultProviderIfPresent) {
            if (previousProjector == null
                    || !(previousProjector instanceof ResultProvider)) {
                CollectingProjector collectingProjector = new CollectingProjector();
                if (previousProjector != null) {
                    previousProjector.downstream(collectingProjector);
                }
                localProjectors.add(collectingProjector);
                previousProjector = collectingProjector;
            }
        }
        return new FlatProjectorChain(localProjectors, resultProviderOptional(previousProjector));
    }

    private static Optional<ResultProvider> resultProviderOptional(@Nullable Projector projector) {
        Optional<ResultProvider> resultProvider;
        if (projector != null && projector instanceof ResultProvider) {
            resultProvider = Optional.of((ResultProvider)projector);
        } else {
            resultProvider = Optional.absent();
        }
        return resultProvider;
    }
}
