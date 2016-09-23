/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect.sources;

import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.node.dql.CollectPhase;

import java.util.Collection;

public class ProjectorSetupCollectSource implements CollectSource {

    private final CollectSource sourceDelegate;
    private final ProjectorFactory projectorFactory;

    public ProjectorSetupCollectSource(CollectSource sourceDelegate, ProjectorFactory projectorFactory) {
        this.sourceDelegate = sourceDelegate;
        this.projectorFactory = projectorFactory;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        if (collectPhase.projections().isEmpty()) {
            return sourceDelegate.getCollectors(collectPhase, downstream, jobCollectContext);
        }
        FlatProjectorChain projectorChain = FlatProjectorChain.withAttachedDownstream(
            projectorFactory,
            jobCollectContext.queryPhaseRamAccountingContext(),
            collectPhase.projections(),
            downstream,
            collectPhase.jobId()
        );
        projectorChain.prepare();
        return sourceDelegate.getCollectors(collectPhase, projectorChain.firstProjector(), jobCollectContext);
    }
}
