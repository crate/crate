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

package io.crate.executor.transport;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Task;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Task which delays the execution of another task until {@code listenableFuture} has completed.
 */
class DelayedTask implements Task {

    private final ListenableFuture<?> listenableFuture;
    private final Task rootTask;

    DelayedTask(ListenableFuture<?> listenableFuture, Task rootTask) {
        this.listenableFuture = listenableFuture;
        this.rootTask = rootTask;
    }

    @Override
    public void execute(final RowReceiver rowReceiver) {
        Futures.addCallback(listenableFuture, new FutureCallback<Object>() {
            @Override
            public void onSuccess(@Nullable Object result) {
                rootTask.execute(rowReceiver);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                rowReceiver.fail(t);
            }
        });
    }

    @Override
    public ListenableFuture<List<Long>> executeBulk() {
        throw new UnsupportedOperationException("MultiPhaseTask doesn't support bulk operations");
    }
}
