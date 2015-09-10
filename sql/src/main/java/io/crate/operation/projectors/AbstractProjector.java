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

package io.crate.operation.projectors;

import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;

public abstract class AbstractProjector implements Projector {

    // these stateCheck classes are used to avoid having to do null-state checks in concrete implementations
    // -> upstream/downstreams are never null (unless a concrete implementions nulls them)
    private final static RowUpstream STATE_CHECK_ROW_UPSTREAM = new StateCheckRowUpstream();
    private final static RowReceiver STATE_CHECK_RECEIVER = new StateCheckReceiver();

    private RowUpstream upstream = STATE_CHECK_ROW_UPSTREAM;
    protected RowReceiver downstream = STATE_CHECK_RECEIVER;
    protected ExecutionState executionState;

    @Override
    public void downstream(RowReceiver rowReceiver) {
        assert rowReceiver != null : "rowReceiver must not be null";
        this.downstream = rowReceiver;
        rowReceiver.setUpstream(this);
    }

    @Override
    public void prepare(ExecutionState executionState) {
        this.executionState = executionState;
    }

    @Override
    public void pause() {
        upstream.pause();
    }

    @Override
    public void resume(boolean async) {
        upstream.resume(async);
    }

    @Override
    public void repeat() {
        upstream.repeat();
    }

    @Override
    public boolean requiresRepeatSupport() {
        return downstream.requiresRepeatSupport();
    }

    @Override
    public void setUpstream(RowUpstream upstream) {
        assert upstream != null : "upstream must not be null";
        this.upstream = upstream;
    }

    private static class StateCheckRowUpstream implements RowUpstream {

        public static final String STATE_ERROR = "upstream not set";

        @Override
        public void pause() {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void resume(boolean async) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void repeat() {
            throw new IllegalStateException(STATE_ERROR);
        }
    }

    private static class StateCheckReceiver implements RowReceiver {

        private static final String STATE_ERROR = "downstream not set";

        @Override
        public boolean setNextRow(Row row) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void finish() {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void fail(Throwable throwable) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void prepare(ExecutionState executionState) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public void setUpstream(RowUpstream upstream) {
            throw new IllegalStateException(STATE_ERROR);
        }

        @Override
        public boolean requiresRepeatSupport() {
            throw new IllegalStateException(STATE_ERROR);
        }
    }
}

