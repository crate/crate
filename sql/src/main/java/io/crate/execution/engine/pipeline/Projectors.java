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

package io.crate.execution.engine.pipeline;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.Projection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

public final class Projectors {

    private final ArrayList<Projector> projectors;
    private final boolean independentScroll;

    public Projectors(Collection<? extends Projection> projections,
                      UUID jobId,
                      RamAccountingContext ramAccountingContext,
                      ProjectorFactory projectorFactory) {
        boolean independentScroll = false;
        this.projectors = new ArrayList<>(projections.size());
        for (Projection projection : projections) {
            if (projection.requiredGranularity().ordinal() > projectorFactory.supportedGranularity().ordinal()) {
                continue;
            }
            Projector projector = projectorFactory.create(projection, ramAccountingContext, jobId);
            projectors.add(projector);
            independentScroll = independentScroll || projector.providesIndependentScroll();
        }
        this.independentScroll = independentScroll;
    }

    public BatchIterator<Row> wrap(BatchIterator<Row> source) {
        BatchIterator<Row> result = source;
        for (Projector projector : projectors) {
            result = projector.apply(result);
        }
        return result;
    }

    public boolean providesIndependentScroll() {
        return independentScroll;
    }
}
