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

package io.crate.executor;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Row;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;

public abstract class JobTask implements Task {

    private final UUID jobId;

    protected JobTask(UUID jobId) {
        this.jobId = jobId;
    }

    public UUID jobId() {
        return this.jobId;
    }


    public static void resultToRowReceiver(final ListenableFuture<? extends TaskResult> resultFuture, final RowReceiver rowReceiver) {
        Futures.addCallback(resultFuture, new FutureCallback<TaskResult>() {
            @Override
            public void onSuccess(@Nullable TaskResult result) {
                if (result != null) {
                    for (Row row : result.rows()) {
                        rowReceiver.setNextRow(row);
                    }
                }
                rowReceiver.finish(RepeatHandle.UNSUPPORTED);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                rowReceiver.fail(t);
            }
        });
    }
}
