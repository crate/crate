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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.crate.breaker.RamAccountingContext;
import io.crate.operation.projectors.*;
import io.crate.planner.projection.Projection;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public class ProjectorChainContext extends AbstractExecutionSubContext {

    private final static Logger LOGGER = Loggers.getLogger(ProjectorChainContext.class);

    private final String name;
    private final RowReceiver rowReceiver;

    public ProjectorChainContext(int id,
                                 String name,
                                 UUID jobId,
                                 ProjectorFactory projectorFactory,
                                 List<Projection> projections,
                                 RowReceiver rowReceiver,
                                 RamAccountingContext ramAccountingContext) {
        super(id, LOGGER);
        this.name = name;
        ListenableRowReceiver listenableRowReceiver = RowReceivers.listenableRowReceiver(rowReceiver);
        Futures.addCallback(listenableRowReceiver.finishFuture(), new FutureCallback<Void>() {
            @Override
            public void onSuccess(@Nullable Void result) {
                ProjectorChainContext.this.close(null);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                ProjectorChainContext.this.close(t);
            }
        });
        FlatProjectorChain projectorChain = FlatProjectorChain.withAttachedDownstream(
            projectorFactory,
            ramAccountingContext,
            projections,
            listenableRowReceiver,
            jobId
        );
        this.rowReceiver = projectorChain.firstProjector();
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        rowReceiver.kill(t);
    }

    @Override
    public String name() {
        return name;
    }

    public RowReceiver rowReceiver() {
        return rowReceiver;
    }

}
