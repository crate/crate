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

package io.crate.execution.engine.collect.sources;

import java.util.concurrent.CompletableFuture;

import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.pipeline.ProjectorFactory;
import io.crate.execution.engine.pipeline.Projectors;
import io.crate.metadata.TransactionContext;

public class ProjectorSetupCollectSource implements CollectSource {

    private final CollectSource sourceDelegate;
    private final ProjectorFactory projectorFactory;

    public ProjectorSetupCollectSource(CollectSource sourceDelegate, ProjectorFactory projectorFactory) {
        this.sourceDelegate = sourceDelegate;
        this.projectorFactory = projectorFactory;
    }

    @Override
    public CompletableFuture<BatchIterator<Row>> getIterator(TransactionContext txnCtx,
                                                             CollectPhase collectPhase,
                                                             CollectTask collectTask,
                                                             boolean supportMoveToStart) {
        var futureSourceIterator = sourceDelegate.getIterator(txnCtx, collectPhase, collectTask, supportMoveToStart);
        return futureSourceIterator.thenApply(it -> Projectors.wrap(
            collectPhase.projections(),
            collectPhase.jobId(),
            collectTask.txnCtx(),
            collectTask.getRamAccounting(),
            collectTask.memoryManager(),
            projectorFactory,
            it
        ));
    }
}
