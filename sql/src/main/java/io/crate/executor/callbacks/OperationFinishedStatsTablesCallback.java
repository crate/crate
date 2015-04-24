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

package io.crate.executor.callbacks;

import com.google.common.util.concurrent.FutureCallback;
import io.crate.breaker.RamAccountingContext;
import io.crate.exceptions.Exceptions;
import io.crate.operation.collect.StatsTables;

import javax.annotation.Nonnull;
import java.util.UUID;


public class OperationFinishedStatsTablesCallback<T> implements FutureCallback<T> {

    private final int operationId;
    private final StatsTables statsTables;
    private final RamAccountingContext ramAccountingContext;

    public OperationFinishedStatsTablesCallback(int operationId,
                                                StatsTables statsTables,
                                                RamAccountingContext ramAccountingContext) {
        this.operationId = operationId;
        this.statsTables = statsTables;
        this.ramAccountingContext = ramAccountingContext;
    }

    @Override
    public void onSuccess(T result) {
        statsTables.operationFinished(operationId, null, ramAccountingContext.totalBytes());
        ramAccountingContext.close();
    }

    @Override
    public void onFailure(@Nonnull Throwable t) {
        statsTables.operationFinished(operationId, Exceptions.messageOf(t), ramAccountingContext.totalBytes());
        ramAccountingContext.close();
    }
}
