/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.jobs;

import io.crate.breaker.RamAccountingContext;
import io.crate.operation.data.BatchConsumer;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

public class ProjectorChainContext extends AbstractExecutionSubContext {

    private final static ESLogger LOGGER = Loggers.getLogger(ProjectorChainContext.class);

    private final String name;
    private final BatchConsumer rowReceiver;

    public ProjectorChainContext(int id,
                                 String name,
                                 UUID jobId,
                                 ProjectorFactory projectorFactory,
                                 List<Projection> projections,
                                 BatchConsumer rowReceiver,
                                 RamAccountingContext ramAccountingContext) {
        super(id, LOGGER);
        this.name = name;
        // XDOBE: implement
//        ListenableRowReceiver listenableRowReceiver = RowReceivers.listenableRowReceiver(rowReceiver);
//        Futures.addCallback(listenableRowReceiver.finishFuture(), new FutureCallback<Void>() {
//            @Override
//            public void onSuccess(@Nullable Void result) {
//                ProjectorChainContext.this.close(null);
//            }
//
//            @Override
//            public void onFailure(@Nonnull Throwable t) {
//                ProjectorChainContext.this.close(t);
//            }
//        });
        BatchConsumer projectorChain = FlatProjectorChain.withAttachedDownstream(
            projectorFactory,
            ramAccountingContext,
            projections,
            //listenableRowReceiver,
            rowReceiver,
            jobId
        );
        this.rowReceiver = projectorChain;
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        // XDOBE: rowReceiver.kill(t);
    }

    @Override
    public String name() {
        return name;
    }

    public BatchConsumer rowReceiver() {
        return rowReceiver;
    }

}
