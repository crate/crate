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
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;

public abstract class AbstractRowPipe implements RowPipe {

    private final static RowUpstream NO_OP_ROW_UPSTREAM = new NoOpRowUpstream();
    private final static RowReceiver NO_OP_RECEIVER = new NoOpReceiver();

    private RowUpstream upstream = NO_OP_ROW_UPSTREAM;
    protected RowDownstreamHandle downstream = NO_OP_RECEIVER;

    @Override
    public void downstream(RowDownstreamHandle rowDownstreamHandle) {
        this.downstream = rowDownstreamHandle;
        if (rowDownstreamHandle instanceof RowReceiver) {
            ((RowReceiver) rowDownstreamHandle).setUpstream(this);
        }
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
    public void setUpstream(RowUpstream upstream) {
        this.upstream = upstream;
    }


    private static class NoOpRowUpstream implements RowUpstream {

        @Override
        public void pause() {}

        @Override
        public void resume(boolean async) {}
    }

    private static class NoOpReceiver implements RowReceiver {

        @Override
        public boolean setNextRow(Row row) {
            return false;
        }

        @Override
        public void finish() {}

        @Override
        public void fail(Throwable throwable) {}

        @Override
        public void setUpstream(RowUpstream upstream) {}
    }
}

