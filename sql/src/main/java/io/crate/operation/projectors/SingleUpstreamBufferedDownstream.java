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

package io.crate.operation.projectors;

import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SingleUpstreamBufferedDownstream extends ForwardingRowReceiver implements RowDownstream, RowUpstream {

    private final List<Object[]> buffer = new ArrayList<>();
    private final Set<Requirement> requirements;

    private RowUpstream upstream;
    private boolean receiverAcquired = false;
    private boolean finished = false;
    private ExecutionState executionState;

    public SingleUpstreamBufferedDownstream(RowReceiver rowReceiver) {
        super(rowReceiver);
        rowReceiver.setUpstream(this);
        requirements = Requirements.remove(rowReceiver.requirements(), Requirement.REPEAT);
    }

    private void bufferRow(Row row) {
        if (finished) {
            throw new IllegalStateException("already finished");
        }
        buffer.add(row.materialize());
    }

    @Override
    public void repeat() {
        if (!finished) {
            throw new IllegalStateException("finished must be called before repeat");
        }
        IterableRowEmitter rowEmitter = new IterableRowEmitter(rowReceiver, executionState, new CollectionBucket(buffer));
        rowEmitter.run();
    }

    @Override
    public void prepare(ExecutionState executionState) {
        this.executionState = executionState;
        super.prepare(executionState);
    }

    @Override
    public Set<Requirement> requirements() {
        return requirements;
    }

    @Override
    public RowReceiver newRowReceiver() {
        if (receiverAcquired) {
            throw new IllegalStateException("newRowReceiver called more than once");
        }
        receiverAcquired = true;
        return this;
    }

    @Override
    public boolean setNextRow(Row row) {
        bufferRow(row);
        return super.setNextRow(row);
    }

    @Override
    public void setUpstream(RowUpstream rowUpstream) {
        this.upstream = rowUpstream;
    }

    @Override
    public void finish() {
        finished = true;
        super.finish();
    }

    @Override
    public void pause() {
        upstream.pause();
    }

    @Override
    public void resume(boolean async) {
        upstream.resume(async);
    }
}
