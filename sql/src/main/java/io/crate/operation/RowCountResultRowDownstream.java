/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.ResultReceiver;
import io.crate.concurrent.CompletionListener;
import io.crate.concurrent.CompletionMultiListener;
import io.crate.core.collections.Row;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.operation.projectors.RowReceiver;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * RowDownstream that will set a TaskResultFuture once the result is ready.
 * It will also close the associated context once it is done
 */
public class RowCountResultRowDownstream implements ResultReceiver {

    private final SettableFuture<TaskResult> result;
    private final List<Object[]> rows = new ArrayList<>();
    private CompletionListener listener = CompletionListener.NO_OP;

    public RowCountResultRowDownstream(SettableFuture<TaskResult> result) {
        this.result = result;
    }

    @Override
    public RowReceiver.Result setNextRow(Row row) {
        rows.add(row.materialize());
        return RowReceiver.Result.CONTINUE;
    }

    @Override
    public void finish() {
        result.set(new RowCountResult(((long) rows.iterator().next()[0])));
        listener.onSuccess(null);
    }

    @Override
    public void fail(@Nonnull Throwable t) {
        result.setException(t);
        listener.onFailure(t);
    }

    @Override
    public void addListener(CompletionListener listener) {
        this.listener = CompletionMultiListener.merge(this.listener, listener);
    }
}
