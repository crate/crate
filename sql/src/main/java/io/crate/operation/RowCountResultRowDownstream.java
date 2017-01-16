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
import io.crate.operation.data.BatchConsumer;
import io.crate.operation.data.BatchCursor;

import javax.annotation.Nonnull;

/**
 * RowDownstream that will set a TaskResultFuture once the result is ready.
 * It will also close the associated context once it is done
 */
public class RowCountResultRowDownstream implements BatchConsumer {

    private final SettableFuture<Long> result;

    public RowCountResultRowDownstream(SettableFuture<Long> result) {
        this.result = result;
    }

    @Override
    public void fail(@Nonnull Throwable t) {
        result.setException(t);
    }

    @Override
    public void accept(BatchCursor batchCursor) {
        assert batchCursor.status() == BatchCursor.Status.ON_ROW: "row count result must not be empty";
        result.set((Long) batchCursor.get(0));
        batchCursor.close();
    }
}
