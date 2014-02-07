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

package io.crate.executor.transport.distributed;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.operator.operations.merge.DownstreamOperation;
import org.cratedb.DataType;

import java.util.concurrent.atomic.AtomicInteger;

public class DownstreamOperationContext {

    private final AtomicInteger mergeOperationsLeft;
    private final DownstreamOperation downstreamOperation;
    private final SettableFuture<Object[][]> listener;
    private final DataType.Streamer<?>[] streamers;

    public DownstreamOperationContext(DownstreamOperation downstreamOperation,
                                      int numUpstreams,
                                      SettableFuture<Object[][]> listener,
                                      DataType.Streamer<?>[] streamers) {
        this.mergeOperationsLeft = new AtomicInteger(numUpstreams);
        this.downstreamOperation = downstreamOperation;
        this.listener = listener;
        this.streamers = streamers;
    }

    public void add(Object[][] rows) {
        downstreamOperation.addRows(rows);

        if (mergeOperationsLeft.decrementAndGet() == 0) {
            listener.set(downstreamOperation.result());
        }
    }

    public DataType.Streamer<?>[] streamers() {
        return streamers;
    }
}
